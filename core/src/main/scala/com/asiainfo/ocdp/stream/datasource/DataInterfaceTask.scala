package com.asiainfo.ocdp.stream.datasource

import com.asiainfo.ocdp.stream.common.StreamingCache
import com.asiainfo.ocdp.stream.config.MainFrameConf
import com.asiainfo.ocdp.stream.event.Event
import com.asiainfo.ocdp.stream.manager.StreamTask
import com.asiainfo.ocdp.stream.service.DataInterfaceServer
import com.asiainfo.ocdp.stream.tools.{ CacheFactory, Json4sUtils }
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import scala.collection.mutable.ArrayBuffer
import scala.collection.{ mutable, immutable }
import org.apache.spark.rdd.RDD
import java.text.SimpleDateFormat

/**
 * Created by leo on 9/16/15.
 */
class DataInterfaceTask(id: String, interval: Int) extends StreamTask {

  val dataInterfaceService = new DataInterfaceServer

  val conf = dataInterfaceService.getDataInterfaceInfoById(id)
  conf.setInterval(interval)

  val labels = dataInterfaceService.getLabelsByIFId(id)

  val events: Array[Event] = dataInterfaceService.getEventsByIFId(id)

  protected def transform(source: String, schema: StructType): Option[Row] = {
    val delim = conf.get("delim", ",")
    val inputArr = (source + delim + "DummySplitHolder").split(delim).dropRight(1).toSeq
    Some(Row.fromSeq(inputArr))
  }

  final def process(ssc: StreamingContext) = {
    //    this.ssc = ssc
    val sqlc = new SQLContext(ssc.sparkContext)

    registFunction(sqlc)

    //1 根据输入数据接口配置，生成数据流 DStream
    val inputStream = readSource(ssc)

    //1.2 根据输入数据接口配置，生成构造 sparkSQL DataFrame 的 structType
    val schema = conf.getBaseSchema
    val udfSchema = conf.getUDFSchema

    //2 流数据处理
    inputStream.foreachRDD(rdd => {
      if (rdd.partitions.length > 0) {
        //2.1 流数据转换

        val rowRDD = rdd.map(inputArr => {
          transform(inputArr, schema)
        }).collect {
          case Some(row) => row
        }

        val df: DataFrame = sqlc.createDataFrame(rowRDD, schema)
        // modified by surq at 2015.11.11 start
        var mixDF: DataFrame = null
        val filter_expr = conf.get("filter_expr").trim()
        if (filter_expr != "") mixDF = df.selectExpr(udfSchema.fieldNames: _*).filter(filter_expr)
        else mixDF = df.selectExpr(udfSchema.fieldNames: _*)
        // deleted by surq at 2015.11.11
        // val mixDF = df.filter(conf.get("filter_expr", "1=1")).selectExpr(udfSchema.fieldNames: _*)
        // modified by surq at 2015.11.11 end
        // add by surq at 2015.11.09 start
        mixDF.persist()
        if (mixDF.count > 0) {
          val enDF = execLabels(mixDF)
          val enhancedDF = enDF._1
          val cacheRDD = enDF._2
          enhancedDF.persist()
          makeEvents(enhancedDF, conf.get("uniqKeys"))
          enhancedDF.unpersist()
        }
        mixDF.unpersist()
        // add by surq at 2015.11.09 end
        // deleted by surq at 2015.11.09 start
        //          df.persist()
        //          val enhancedDF = execLabels(mixDF)
        //          df.unpersist()
        //
        //          enhancedDF.persist
        //          makeEvents(enhancedDF, conf.get("uniqKeys"))
        //          //        subscribeEvents(eventMap)
        //          enhancedDF.unpersist()
        // deleted by surq at 2015.11.09 end
      }
    })
  }

  def registFunction(sqlc: SQLContext) {
    sqlc.udf.register("Conv", (data: String) => {
      var lc = Integer.toHexString(data.toInt).toUpperCase
      while (lc.length < 4) {
        lc = "0" + lc
      }
      lc
    })

    sqlc.udf.register("concat", (firststr: String, secondstr: String) => firststr + secondstr)

    sqlc.udf.register("from_unixtime", (date: String, format: String) => {
      val df = new SimpleDateFormat(format);
      df.parse(date).toString();
    })

    sqlc.udf.register("unix_timestamp", () => System.currentTimeMillis().toString)
    sqlc.udf.register("currenttimesub", (subduction: Int) => (System.currentTimeMillis - subduction).toString)
    sqlc.udf.register("currenttimeadd", (add: Int) => (System.currentTimeMillis + add) .toString)
  }

  final def readSource(ssc: StreamingContext): DStream[String] = {
    StreamingInputReader.readSource(ssc, conf)
  }

  def execLabels(df: DataFrame): (DataFrame, RDD[String]) = {
    println(" Begin exec labes : " + System.currentTimeMillis())

    val jsonRDD = df.toJSON

    val batchLimit = MainFrameConf.systemProps.getInt("cacheQryBatchSizeLimit")

    val enhancedJsonRDD = jsonRDD.mapPartitions(iter => {
      new Iterator[String] {
        private[this] var currentPos: Int = -1
        private[this] var arrayBuffer: Array[String] = _

        override def hasNext: Boolean = (currentPos != -1 && currentPos < arrayBuffer.length) || (iter.hasNext && fetchNext())

        override def next(): String = {
          currentPos += 1
          arrayBuffer(currentPos - 1)
        }

        private final def fetchNext(): Boolean = {
          val currentArrayBuffer = new ArrayBuffer[String]
          currentPos = -1
          var totalFetch = 0
          var result = false

          val totaldata = mutable.MutableList[Map[String, String]]()
          val minimap = mutable.Map[String, Map[String, String]]()

          val labelQryKeysSet = mutable.Set[String]()

          while (iter.hasNext && (totalFetch < batchLimit)) {
            //            println(iter.next())
            val currentLine = Json4sUtils.jsonStr2Map(iter.next())
            totaldata += currentLine

            val uk = conf.get("uniqKeys").split(",").map(currentLine(_)).mkString(",")

            minimap += ("Label:" + uk -> currentLine)

            labels.foreach(label => {
              val qryKeys = label.getQryKeys(currentLine)
              if (qryKeys != null && qryKeys.nonEmpty) {
                labelQryKeysSet ++= qryKeys
              }
            })

            totalFetch += 1
            currentPos = 0
            result = true
          }

          println(" partition data size = " + totalFetch)

          val f1 = System.currentTimeMillis()
          var cachemap_old: Map[String, Any] = null
          try {
            cachemap_old = CacheFactory.getManager.getMultiCacheByKeys(minimap.keys.toList).toMap
          } catch {
            case ex: Exception =>
              logError("= = " * 15 + " got exception in EventSource while get cache")
              throw ex
          }

          val f2 = System.currentTimeMillis()
          println(" query label cache data cost time : " + (f2 - f1) + " millis ! ")

          val labelQryData = CacheFactory.getManager.hgetall(labelQryKeysSet.toList)

          val f3 = System.currentTimeMillis()
          println(" query label need data cost time : " + (f3 - f2) + " millis ! ")

          val cachemap_new = mutable.Map[String, Any]()
          totaldata.foreach(x => {
            val uk = conf.get("uniqKeys").split(",").map(x(_)).mkString(",")

            val key = "Label:" + uk
            var value = x

            var rule_caches = cachemap_old.get(key).get match {
              case cache: immutable.Map[String, StreamingCache] => cache
              case null =>
                val cachemap = mutable.Map[String, StreamingCache]()
                labels.foreach(label => {
                  cachemap += (label.conf.getId -> null)
                })
                cachemap.toMap
            }

            labels.foreach(label => {
              val cacheOpt = rule_caches.get(label.conf.getId)
              var old_cache: StreamingCache = null
              if (cacheOpt != None) old_cache = cacheOpt.get

              val resultTuple = label.attachLabel(value, old_cache, labelQryData)
              value = resultTuple._1
              val newcache = resultTuple._2
              rule_caches = rule_caches.updated(label.conf.getId, newcache)
            })
            currentArrayBuffer.append(Json4sUtils.map2JsonStr(value))

            cachemap_new += (key -> rule_caches.asInstanceOf[Any])
          })

          val f4 = System.currentTimeMillis()
          println(" Exec labels cost time : " + (f4 - f3) + " millis ! ")

          //update caches to CacheManager
          CacheFactory.getManager.setMultiCache(cachemap_new)
          println(" update labels cache cost time : " + (System.currentTimeMillis() - f4) + " millis ! ")

          arrayBuffer = currentArrayBuffer.toArray
          result
        }
      }
    })

    enhancedJsonRDD.persist()
    (df.sqlContext.read.json(enhancedJsonRDD), enhancedJsonRDD)
  }

  final def makeEvents(df: DataFrame, uniqKeys: String) = {
    println(" Begin exec evets : " + System.currentTimeMillis())

    events.map(event => event.buildEvent(df, uniqKeys))

  }
}
