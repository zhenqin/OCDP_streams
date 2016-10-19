package com.asiainfo.ocdp.stream.datasource


import com.asiainfo.ocdp.stream.common.StreamingCache
import com.asiainfo.ocdp.stream.event.Event
import com.asiainfo.ocdp.stream.manager.StreamTask
import com.asiainfo.ocdp.stream.service.DataInterfaceServer
import com.asiainfo.ocdp.stream.tools.{ CacheFactory, Json4sUtils }
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ArrayBuffer
import scala.collection.{ mutable, immutable }
import org.apache.spark.rdd.RDD
import java.text.SimpleDateFormat
import java.util.concurrent.ExecutorCompletionService
import com.asiainfo.ocdp.stream.tools.CacheQryThreadPool
import java.util.concurrent.Callable

/**
 * Created by surq on 12/09/15
 */
class DataInterfaceTask(id: String, interval: Int) extends StreamTask {
  val dataInterfaceService = new DataInterfaceServer

  val conf = dataInterfaceService.getDataInterfaceInfoById(id)
  val labels = dataInterfaceService.getLabelsByIFId(id)
  val events: Array[Event] = dataInterfaceService.getEventsByIFId(id)
  conf.setInterval(interval)
  // 原始信令字段个数
  val baseItemSize = conf.getBaseItemsSize

  protected def transform(source: String, schema: StructType): Option[Row] = {
    val delim = conf.get("delim", ",")
    val inputArr = (source + delim + "DummySplitHolder").split(delim).dropRight(1).toSeq
    if (inputArr.size != baseItemSize) None else Some(Row.fromSeq(inputArr))
  }
  //  val kv = Json4sUtils.jsonStr2ArrMap(propsJsonStr, "fields")
  final def process(ssc: StreamingContext) = {
    //    this.ssc = ssc
    val sqlc = new SQLContext(ssc.sparkContext)
    // 用户自定义sql的方法
    registFunction(sqlc)

    //1 根据输入数据接口配置，生成数据流 DStream
    val inputStream = readSource(ssc)

    //1.2 根据输入数据接口配置，生成构造 sparkSQL DataFrame 的 structType
    val schema = conf.getBaseSchema
    // 全量字段: baseItems + udfItems 
    val allItemsSchema = conf.getAllItemsSchema
    //2 流数据处理
    val transFormRDD = inputStream.foreach(rdd => {
      val t0 = System.currentTimeMillis()
      //2.1 流数据转换
      val rowRDD = rdd.map(inputArr => transform(inputArr, schema)).collect { case Some(row) => row }
      if (rowRDD.partitions.size > 0) {
        val t1 = System.currentTimeMillis()
        println("1.kafka RDD 转换成 rowRDD 耗时 (millis):" + (t1 - t0))

        val dataFrame = sqlc.createDataFrame(rowRDD, schema)

        val t2 = System.currentTimeMillis
        println("2.rowRDD 转换成 DataFrame 耗时 (millis):" + (t2 - t1))
        val filter_expr = conf.get("filter_expr")

        val mixDF = if (filter_expr != null && filter_expr.trim != "") dataFrame.selectExpr(allItemsSchema.fieldNames: _*).filter(filter_expr)
        else dataFrame.selectExpr(allItemsSchema.fieldNames: _*)


        val t3 = System.currentTimeMillis
        println("3.DataFrame 最初过滤不规则数据耗时 (millis):" + (t3 - t2))
        val labelRDD = execLabels(mixDF)

        //-------------------持久化 yang修改
        labelRDD.persist(StorageLevel.MEMORY_AND_DISK)
        //---------------------

        val t4 = System.currentTimeMillis
        println("4.dataframe 转成rdd打标签耗时(millis):" + (t4 - t3))

        // read.json为spark sql 动作类提交job
        val enhancedDF = sqlc.read.json(labelRDD)

        val t5 = System.currentTimeMillis
        println("5.RDD 转换成 DataFrame 耗时(millis):" + (t5 - t4))

        makeEvents(enhancedDF, conf.get("uniqKeys"))
	      println("event uniqKeys:"+conf.get("uniqKeys"))
        println("6.所有业务营销 耗时(millis):" + (System.currentTimeMillis - t5))

        //---------yang 修改--------
        labelRDD.unpersist()
        //-----------------
      }
    })
  }

  /**
   * 自定义slq 方法注册
   */
  def registFunction(sqlc: SQLContext) {
    sqlc.udf.register("Conv", (data: String) => {
      val lc = Integer.toHexString(data.toInt).toUpperCase
      if (lc.length < 4) { val concatStr = "0000" + lc; concatStr.substring(concatStr.length - 4) } else lc
    })

    sqlc.udf.register("concat", (firststr: String, secondstr: String) => firststr + secondstr)
    sqlc.udf.register("from_unixtime", (date: String, format: String) => (new SimpleDateFormat(format)).parse(date).toString())
    sqlc.udf.register("unix_timestamp", () => System.currentTimeMillis().toString)
    sqlc.udf.register("currenttimesub", (subduction: Int) => (System.currentTimeMillis - subduction).toString)
    sqlc.udf.register("currenttimeadd", (add: Int) => (System.currentTimeMillis + add).toString)
  }

  final def readSource(ssc: StreamingContext): DStream[String] = {
    StreamingInputReader.readSource(ssc, conf)
  }

  /**
   * 字段增强：根据uk（各个数据接口定义的业务主键uniqKeys,eg:imsi）从codis中取出相关关联数据，进行打标签操作
   */
  def execLabels(df: DataFrame): RDD[String] = {
    df.toJSON.mapPartitions(iter => {
      val qryCacheService = new ExecutorCompletionService[List[(String, Array[Byte])]](CacheQryThreadPool.threadPool)
      val hgetAllService = new ExecutorCompletionService[Seq[(String, java.util.Map[String, String])]](CacheQryThreadPool.threadPool)

      // 装载整个批次事件计算中间结果缓存值　label:uk -> 每条信令用map装载
      val busnessKeyList = ArrayBuffer[(String, Map[String, String])]()
      // 装载整个批次打标签操作时，所需要的跟codis数据库交互的key
      val labelQryKeysSet = mutable.Set[String]()
      val cachemap_new = mutable.Map[String, Any]()
      val ukUnion = conf.get("uniqKeys").split(":")//eg:imsi
      iter.toList.map(jsonStr => {
        val currentLine = Json4sUtils.jsonStr2Map(jsonStr)
        val uk = ukUnion.map(currentLine(_)).mkString(",")//uk:从jsonstr中获取到uniqKeys（eg:imsi）的取值，uk=用户imsi号码
        busnessKeyList += ("Label:" + uk -> currentLine)
        // 取出本条数据在打所有标签时所用的查询cache用到的key放入labelQryKeysSet
        labels.foreach(label => {
          val qryKeys = label.getQryKeys(currentLine)//每种标签分别获取codis的查询key
          if (qryKeys != null && qryKeys.nonEmpty) labelQryKeysSet ++= qryKeys
        })
      })
      // cachemap_old
      val f1 = System.currentTimeMillis()
      var cachemap_old: Map[String, Any] = null
      //add liuyu,zhenqin, toSet toList 去重复一次
      val keyList = busnessKeyList.map(line => line._1).toSet.toList
      val batchSize = keyList.size
      println("本批次记录条数：" + batchSize)
      try {
        cachemap_old = CacheFactory.getManager.getMultiCacheByKeys(keyList, qryCacheService).toMap
	 println("cache map size:" + cachemap_old.size)
      } catch {
        case ex: Exception =>
          logError("= = " * 15 + " got exception in EventSource while get cache")
          throw ex
      }
      val f2 = System.currentTimeMillis()
      println(" 1. 查取一批数据缓存中的交互状态信息 cost time : " + (f2 - f1) + " millis ! ")
      val labelQryData = CacheFactory.getManager.hgetall(labelQryKeysSet.toList, hgetAllService)
      val f3 = System.currentTimeMillis()
      println(" 2. 查取此批数据缓存中的用户相关信息表 cost time : " + (f3 - f2) + " millis ! ")
      // 遍历整个批次的数据，逐条记录打标签
      val jsonList = busnessKeyList.map(enum => {
        // 格式 【"Label:" + uk】
        val key = enum._1
        var value = enum._2
        // 从cache中取出本条记录的中间计算结果值
        var rule_caches = cachemap_old.get(key) match {
          case Some(cache) => cache.asInstanceOf[immutable.Map[String, StreamingCache]]
          case None =>
            val cachemap = mutable.Map[String, StreamingCache]()
            labels.foreach(label => cachemap += (label.conf.getId -> null))
            cachemap.toMap
        }
        // 遍历所有所打标签，从cache中取出本条记录对应本标签的中间缓存值，并打标签操作
        labels.foreach(label => {
          // 从cache中取出本条记录所关联的所有标签所用到的用户资料表［静态表］
          val old_cache = rule_caches.get(label.conf.getId) match {
            case Some(cache) => cache
            case None => null
          }
          // 传入本记录、往期中间记算结果cache、相关的用户资料表，进行打标签操作
          val resultTuple = label.attachLabel(value, old_cache, labelQryData)
          // 增强记录信息，加标签字段
          value = resultTuple._1
          // 更新往期中间记算结果cache
          val newcache = resultTuple._2
          rule_caches = rule_caches.updated(label.conf.getId, newcache)
        })
        // 更新往期中间记算结果cache【"Label:" + uk-> {labelId->rule_caches}】
        cachemap_new += (key -> rule_caches.asInstanceOf[Any])
        Json4sUtils.map2JsonStr(value)
      })

      val f4 = System.currentTimeMillis()
      println(" 3. 遍历一批次数据并打相关联的标签 cost time : " + (f4 - f3) + " millis ! ")
      //update caches to CacheManager
      CacheFactory.getManager.setMultiCache(cachemap_new)
      println(" 4. 更新这批数据的缓存中的交互状态信息 cost time : " + (System.currentTimeMillis() - f4) + " millis ! ")
      jsonList.iterator
    })
  }

  /**
   * 业务处理
   */
  final def makeEvents(df: DataFrame, uniqKeys: String) = {
    println(" Begin persist dataFrame : " + System.currentTimeMillis())
    df.persist(StorageLevel.MEMORY_AND_DISK)
    println(" persist end : " + System.currentTimeMillis())
    events.map(event =>{
      println(" Begin exec evets : " + System.currentTimeMillis())
      event.buildEvent(df, uniqKeys)
      println(" end exec evets : " + System.currentTimeMillis())
    })
    println(" all evnt exec end : " + System.currentTimeMillis())
    df.unpersist
    println(" dataFrame unpersist end : " + System.currentTimeMillis())
  }
}
