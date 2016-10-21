package com.asiainfo.ocdp.stream.event

import com.asiainfo.ocdp.stream.config.{ EventConf, MainFrameConf }
import com.asiainfo.ocdp.stream.constant.EventConstant
import com.asiainfo.ocdp.stream.service.EventServer
import com.asiainfo.ocdp.stream.tools.{ Json4sUtils, StreamWriterFactory }
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD

/**
 * Created by surq on 12/09/15
 */
class Event extends Serializable {

  var conf: EventConf = null

  val eventServer = new EventServer()
  val batchLimit = MainFrameConf.systemProps.getInt("cacheQryBatchSizeLimit")

  def init(eventconf: EventConf) {
    conf = eventconf
  }

  def buildEvent(df: DataFrame, uniqKeys: String) = {
    // 用户配置的本业务输出字段
    var mix_sel_expr = conf.select_expr.split(",")
    // 把主键追加到业务输出字段列中。查看用户配置的本业务输出字段中是否包括主键字段，如果没有则追加
    uniqKeys.split(":").map(key => mix_sel_expr = if (!mix_sel_expr.contains(key)) mix_sel_expr :+ key else mix_sel_expr)

    // 向输出字段中追加用字户定义字段，如固定字段“1”，“xxx”
    if (conf.get("ext_fields", null) != null)
      mix_sel_expr = mix_sel_expr ++ conf.get("ext_fields", null).split(",")

    // 根据业务条件过滤，并查询出输出字段
    val eventDF = df.filter(conf.filte_expr).selectExpr(mix_sel_expr: _*)

    // 事件复用的时候会用到，注意做eventDF.persist
    if (EventConstant.NEEDCACHE == conf.getInt("needcache", 0)) cacheEvent(eventDF, uniqKeys)

    println("event: " + conf.getId + "/" + conf.getName + " interval: " + conf.interval)

    // 如果业务输出周期不为0，那么需要从codis中取出比兑营销时间，满足条件的输出
    val jsonRDD = if (EventConstant.RealtimeTransmission != conf.interval) checkEvent(eventDF, uniqKeys)
    else eventDF.toJSON
    outputEvent(jsonRDD, uniqKeys)
  }

  /**
   * eventDF:根据业务条件过滤，并查询出的输出字段
   * uniqKeys:主键
   */
  def cacheEvent(eventDF: DataFrame, uniqKeys: String) {
    eventDF.toJSON.mapPartitions(iterator => {
      val t1 = System.currentTimeMillis()
      // batchArrayBuffer中盛放最大batchLimit条数据
      var batchArrayBuffer: ArrayBuffer[(String, String, String)] = null
      val jsonList = iterator.toList
      val size = jsonList.size
      // 把json按指定的与codis库查询条数（batchLimit），分块更新
      val keylists = for (index <- 0 until size) yield {
        if (index % batchLimit == 0) {
          // 把list放入线程池更新codis
          if (index != 0) eventServer.cacheEventData(batchArrayBuffer.toArray)
          batchArrayBuffer = new ArrayBuffer[(String, String, String)]()
        }
        // 解析json数据，拼凑eventKey
        val line = jsonList(index)
        val current = Json4sUtils.jsonStr2Map(line)
        val eventKeyValue = uniqKeys.split(":").map(current(_)).mkString(":")

        // ( eventCache:unikey1:unikey2,Row:eventId:eventID,json)
        batchArrayBuffer += ((EventConstant.EVENT_CACHE_PREFIX_NAME + eventKeyValue,
          EventConstant.EVENTCACHE_FIELD_ROWEVENTID_PREFIX_KEY + conf.id, line))
        // 把list放入线程池更新codis
        if (index == size - 1) eventServer.cacheEventData(batchArrayBuffer.toArray)
        println("mapPartitions " + size + " key cost " + (System.currentTimeMillis() - t1) + " Millis")
        // 返回值
        eventKeyValue
      }
      keylists.toList.iterator
    }).count()
  }

  /**
   * 过滤营销周期不满足的数据，输出需要营销的数据，并更新codis营销时间
   */
  import scala.collection.immutable
  import java.util.concurrent.ExecutorCompletionService
  import com.asiainfo.ocdp.stream.tools.CacheQryThreadPool
  def checkEvent(eventDF: DataFrame, uniqKeys: String): (RDD[String]) = {
    val time_EventId = EventConstant.EVENTCACHE_FIELD_TIMEEVENTID_PREFIX_KEY + conf.id

    println("timeEvent: " + time_EventId)

    eventDF.toJSON.mapPartitions(iter => {
      val eventCacheService = new ExecutorCompletionService[immutable.Map[String, (String, Array[Byte])]](CacheQryThreadPool.threadPool)
      val batchList = new ArrayBuffer[Array[(String, String)]]()

      println("================eventDF.toJSON.mapPartitions=================== ")
      var batchArrayBuffer: ArrayBuffer[(String, String)] = null
      val jsonList = iter.toList
      val size = jsonList.size
      // 把json按指定的与codis库查询条数（batchLimit），分块更新
      for (index <- 0 until size) {
        if (index % batchLimit == 0) {
          // 把list放入线程池更新codis
          if (index != 0) {
            batchList += batchArrayBuffer.toArray
          }
          batchArrayBuffer = new ArrayBuffer[(String, String)]()
        }
        // 解析json数据，拼凑eventKey
        val line = jsonList(index)
        val current = Json4sUtils.jsonStr2Map(line)
        val eventKeyValue = uniqKeys.split(":").map(current(_)).mkString(":")
        // (eventCache:eventKeyValue,jsonValue)
        batchArrayBuffer += ((EventConstant.EVENT_CACHE_PREFIX_NAME + eventKeyValue, line))

        // 把list放入线程池更新codis
        if (index == size - 1) {
          batchList += batchArrayBuffer.toArray
        }
      }

      //todo liuhuan 修改：batchList 是最后要输出匹配的 hgetall 的 key   示例：eventCache:460020033977918
      //todo liuhuan 修改：time_EventId 为输出时 hgetall 的 第一个字段 Time:eventId:81934e5455506507015610543c2b0002
      //todo liuhuan 修改：interval 为 2592000 秒
      val outPutList = eventServer.getEventCache(eventCacheService, batchList.toArray, time_EventId, conf.getInterval)

      val outPutJsonList = scala.collection.mutable.Map[String,String]()
      outPutList.map(line=>{
                // 解析json数据，拼凑eventKey，去除重复营销的key
        val current = Json4sUtils.jsonStr2Map(line)
        val eventKeyValue = uniqKeys.split(":").map(current(_)).mkString(":")
        outPutJsonList += (eventKeyValue -> line)
      })
      val output = outPutJsonList.map(line =>line._2).toList
      output.iterator
    })
  }

  /**
   * 从codis中取出此uk的营销周期信息，查看时间是否满足营销周期，返满足的数据
   * array:(eventCache:eventKeyValue,jsonValue)
   */

  /**
   * rdd格式流输出
   */
  def outputEvent(rdd: RDD[String], uniqKeys: String) = {
    conf.outIFIds.map(ifconf => {
      val writer = StreamWriterFactory.getWriter(ifconf)
      writer.push(rdd, conf, uniqKeys)
    })
  }
}