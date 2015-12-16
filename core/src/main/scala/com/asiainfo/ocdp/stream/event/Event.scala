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

  //  def buildEvent(df: DataFrame, uniqKeys: String) {
  //    // modifed by surq at 2015.11.24 start
  //    //var mix_sel_expr = uniqKeys.split(":") ++ conf.select_expr.split(",")
  //    val keys = uniqKeys.split(":")
  //    var mix_sel_expr = conf.select_expr.split(",")
  //    keys.map(key => mix_sel_expr = if (!mix_sel_expr.contains(key)) mix_sel_expr :+ key else mix_sel_expr)
  //
  //    // modifed by surq at 2015.11.24 end
  //    if (conf.get("ext_fields", null) != null)
  //      mix_sel_expr = mix_sel_expr ++ conf.get("ext_fields", null).split(",")
  //
  //    var eventDF = df.filter(conf.filte_expr).selectExpr(mix_sel_expr: _*)
  //    var jsonRDD: RDD[String] = null
  //    
  //    if (EventConstant.NEEDCACHE == conf.getInt("needcache", 0)) {
  //      cacheEvent(eventDF, uniqKeys)
  //    }
  //    // conf.interval:业务输出周期
  //    if (EventConstant.RealtimeTransmission != conf.interval) {
  //      val tuple = checkEvent(eventDF, uniqKeys)
  //      eventDF = tuple._1
  //      jsonRDD = tuple._2
  //    }
  //
  //    outputEvent(eventDF, uniqKeys)
  //    
  //    if (jsonRDD != null)
  //      jsonRDD.unpersist()
  //
  //  }

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

    // 如果业务输出周期不为0，那么需要从codis中取出比兑营销时间，满足条件的输出
    val jsonRDD = if (EventConstant.RealtimeTransmission != conf.interval) checkEvent(eventDF, uniqKeys)
    else eventDF.toJSON
    outputEvent(jsonRDD, uniqKeys)
  }

  //  def cacheEvent(eventDF: DataFrame, uniqKeys: String) {
  //    val jsonRDD = eventDF.toJSON
  //    jsonRDD.mapPartitions({ iter =>
  //      {
  //        new Iterator[String] {
  //          private[this] var currentPos: Int = -1
  //          private[this] val batchArrayBuffer = new ArrayBuffer[(String, String, String)]()
  //
  //          override def hasNext: Boolean = (currentPos != -1 && currentPos < batchArrayBuffer.length) || (iter.hasNext && batchNext())
  //
  //          override def next(): String = {
  //            currentPos += 1
  //            batchArrayBuffer(currentPos - 1)._3
  //          }
  //
  //          //批量处理
  //          def batchNext(): Boolean = {
  //            currentPos = -1
  //            var batchSize = 0
  //            var result = false
  //
  //            batchArrayBuffer.clear()
  //
  //            //获取一个批次处理的row
  //            while (iter.hasNext && (batchSize < batchLimit)) {
  //              val jsonValue = iter.next()
  //              val current = Json4sUtils.jsonStr2Map(jsonValue)
  //              val eventKeyValue = uniqKeys.split(":").map(current(_)).mkString(":")
  //              batchArrayBuffer.append((EventConstant.EVENT_CACHE_PREFIX_NAME + eventKeyValue,
  //                EventConstant.EVENTCACHE_FIELD_ROWEVENTID_PREFIX_KEY + conf.id, jsonValue))
  //
  //              batchSize += 1
  //            }
  //
  //            //构建一个保存线程，提交一个批次的数据
  //            if (batchArrayBuffer.length > 0) {
  //              result = true
  //              currentPos = 0
  //              eventServer.cacheEventData(batchArrayBuffer.toArray)
  //            }
  //            result
  //          }
  //        }
  //      }
  //    }).count()
  //  }

  //  def checkEvent(eventDF: DataFrame, uniqKeys: String): (DataFrame, RDD[String]) = {
  //    var jsonRDD = eventDF.toJSON
  //    jsonRDD = jsonRDD.mapPartitions(iter => {
  //      new Iterator[String] {
  //        private[this] var currentPos: Int = -1
  //        private[this] var resultBuffer = new ArrayBuffer[(String, String)]()
  //
  //        override def hasNext: Boolean = (currentPos != -1 && currentPos < resultBuffer.length) || (iter.hasNext && batchNext())
  //
  //        override def next(): String = {
  //          currentPos += 1
  //          resultBuffer(currentPos - 1)._2
  //        }
  //
  //        //批量处理
  //        def batchNext(): Boolean = {
  //          currentPos = -1
  //          var batchSize = 0
  //          resultBuffer.clear()
  //          val batchArrayBuffer = new ArrayBuffer[(String, Array[String])]()
  //
  //          //获取一个批次处理的row
  //          while (iter.hasNext && (batchSize < batchLimit)) {
  //            val jsonValue = iter.next()
  //            val current = Json4sUtils.jsonStr2Map(jsonValue)
  //            val eventKeyValue = uniqKeys.split(":").map(current(_)).mkString(":")
  //            batchArrayBuffer.append((EventConstant.EVENT_CACHE_PREFIX_NAME + eventKeyValue,
  //              Array(EventConstant.EVENTCACHE_FIELD_TIMEEVENTID_PREFIX_KEY + conf.id)))
  //            resultBuffer.append((EventConstant.EVENT_CACHE_PREFIX_NAME + eventKeyValue, jsonValue))
  //            batchSize += 1
  //          }
  //
  //          //构建一个保存线程，提交一个批次的数据
  //          if (batchArrayBuffer.length > 0) {
  //            val eventTimes = eventServer.getEventCache(batchArrayBuffer.toArray)
  //
  //            val updateArrayBuffer = new ArrayBuffer[(String, String, String)]()
  //            val current_time = System.currentTimeMillis()
  //            val outputKeys = eventTimes.filter(event => {
  //
  //              if (event._2.size == 0) {
  //                updateArrayBuffer.append((event._1, EventConstant.EVENTCACHE_FIELD_TIMEEVENTID_PREFIX_KEY + conf.id, String.valueOf(current_time)))
  //                true
  //              } else {
  //                val time_field = event._2.head._1
  //                val old_time = event._2.head._2
  //                if (current_time >= (old_time.toLong + conf.getInterval * 1000)) {
  //                  updateArrayBuffer.append((event._1, time_field, String.valueOf(current_time)))
  //                  true
  //                } else false
  //              }
  //            }).keySet
  //
  //            eventServer.cacheEventData(updateArrayBuffer.toArray)
  //
  //            resultBuffer = resultBuffer.dropWhile(x => !outputKeys.contains(x._1))
  //
  //          }
  //
  //          if (resultBuffer.length > 0) {
  //            currentPos = 0
  //            true
  //          } else false
  //
  //        }
  //      }
  //    })
  //
  //    jsonRDD.persist()
  //    (eventDF.sqlContext.read.json(jsonRDD), jsonRDD)
  //  }

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

  //    def checkEvent(eventDF: DataFrame, uniqKeys: String): (DataFrame, RDD[String]) = {
  //      var jsonRDD = eventDF.toJSON
  //      jsonRDD = jsonRDD.mapPartitions(iter => {
  //        new Iterator[String] {
  //          private[this] var currentPos: Int = -1
  //          private[this] var resultBuffer = new ArrayBuffer[(String, String)]()
  //  
  //          override def hasNext: Boolean = (currentPos != -1 && currentPos < resultBuffer.length) || (iter.hasNext && batchNext())
  //  
  //          override def next(): String = {
  //            currentPos += 1
  //            resultBuffer(currentPos - 1)._2
  //          }
  //  
  //          //批量处理
  //          def batchNext(): Boolean = {
  //            currentPos = -1
  //            var batchSize = 0
  //            resultBuffer.clear()
  //            val batchArrayBuffer = new ArrayBuffer[(String, Array[String])]()
  //  
  //            //获取一个批次处理的row
  //            while (iter.hasNext && (batchSize < batchLimit)) {
  //              val jsonValue = iter.next()
  //              val current = Json4sUtils.jsonStr2Map(jsonValue)
  //              val eventKeyValue = uniqKeys.split(":").map(current(_)).mkString(":")
  //              batchArrayBuffer.append((EventConstant.EVENT_CACHE_PREFIX_NAME + eventKeyValue,
  //                Array(EventConstant.EVENTCACHE_FIELD_TIMEEVENTID_PREFIX_KEY + conf.id)))
  //              resultBuffer.append((EventConstant.EVENT_CACHE_PREFIX_NAME + eventKeyValue, jsonValue))
  //              batchSize += 1
  //            }
  //  
  //            //构建一个保存线程，提交一个批次的数据
  //            if (batchArrayBuffer.length > 0) {
  //              val eventTimes = eventServer.getEventCache(batchArrayBuffer.toArray)
  //  
  //              val updateArrayBuffer = new ArrayBuffer[(String, String, String)]()
  //              val current_time = System.currentTimeMillis()
  //              val outputKeys = eventTimes.filter(event => {
  //  
  //                if (event._2.size == 0) {
  //                  updateArrayBuffer.append((event._1, EventConstant.EVENTCACHE_FIELD_TIMEEVENTID_PREFIX_KEY + conf.id, String.valueOf(current_time)))
  //                  true
  //                } else {
  //                  val ss = event._2
  //                  val time_field = event._2.head._1
  //                  val old_time = event._2.head._2
  //                  if (current_time >= (old_time.toLong + conf.getInterval * 1000)) {
  //                    updateArrayBuffer.append((event._1, time_field, String.valueOf(current_time)))
  //                    true
  //                  } else false
  //                }
  //              }).keySet
  //  
  //              eventServer.cacheEventData(updateArrayBuffer.toArray)
  //  
  //              resultBuffer = resultBuffer.dropWhile(x => !outputKeys.contains(x._1))
  //  
  //            }
  //  
  //            if (resultBuffer.length > 0) {
  //              currentPos = 0
  //              true
  //            } else false
  //  
  //          }
  //        }
  //      })
  //  
  //      jsonRDD.persist()
  //      (eventDF.sqlContext.read.json(jsonRDD), jsonRDD)
  //    }

  /**
   * 过滤营销周期不满足的数据，输出需要营销的数据，并更新codis营销时间
   */
  import scala.collection.immutable
  import java.util.concurrent.ExecutorCompletionService
  import com.asiainfo.ocdp.stream.tools.CacheQryThreadPool
  def checkEvent(eventDF: DataFrame, uniqKeys: String): (RDD[String]) = {
    val time_EventId = EventConstant.EVENTCACHE_FIELD_TIMEEVENTID_PREFIX_KEY + conf.id
    eventDF.toJSON.mapPartitions(iter => {
      val eventCacheService = new ExecutorCompletionService[immutable.Map[String, (String, Array[Byte])]](CacheQryThreadPool.threadPool)
      val batchList = new ArrayBuffer[Array[(String, String)]]()

      var batchArrayBuffer: ArrayBuffer[(String, String)] = null
      val jsonList = iter.toList
      val size = jsonList.size
      // 把json按指定的与codis库查询条数（batchLimit），分块更新
      for (index <- 0 until size) {
        if (index % batchLimit == 0) {
          // 把list放入线程池更新codis
          if (index != 0) batchList += batchArrayBuffer.toArray
          batchArrayBuffer = new ArrayBuffer[(String, String)]()
        }
        // 解析json数据，拼凑eventKey
        val line = jsonList(index)
        val current = Json4sUtils.jsonStr2Map(line)
        val eventKeyValue = uniqKeys.split(":").map(current(_)).mkString(":")
        // (eventCache:eventKeyValue,jsonValue)
        batchArrayBuffer += ((EventConstant.EVENT_CACHE_PREFIX_NAME + eventKeyValue, line))

        // 把list放入线程池更新codis
        if (index == size - 1) batchList += batchArrayBuffer.toArray
      }
      val outPutJsonList = eventServer.getEventCache(eventCacheService, batchList.toArray, time_EventId, conf.getInterval)
      outPutJsonList.iterator
    })
  }

  /**
   * 从codis中取出此uk的营销周期信息，查看时间是否满足营销周期，返满足的数据
   * array:(eventCache:eventKeyValue,jsonValue)
   */
  //  def batchFun(batchList: Array[Array[(String, String)]], eventId: String) = {
  //
  //    // 从codis中取出一个limitSize大小的数据营销周期信息
  //    // Map[ventcach:eventKeyvalue->Map(Time:eventId:thisEventID值->time)]
  //        batchList.foreach(batch =>{
  //           eventServer.getEventCache(batch, eventId)
  //          
  //        })
  //        val eventTimes = eventServer.getEventCache(array, eventId)
  //
  //    val updateArrayBuffer = new ArrayBuffer[(String, String, String)]()
  //    val current_time = System.currentTimeMillis
  //    val outputKeys = eventTimes.filter(event => {
  //
  //      // codis中无此条数据的业务周期登记，则新建codis key 插入一条，并营销
  //      if (event._2.size == 0) {
  //        updateArrayBuffer.append((event._1, EventConstant.EVENTCACHE_FIELD_TIMEEVENTID_PREFIX_KEY + conf.id, String.valueOf(current_time)))
  //        true
  //      } else {
  //        val time_field = event._2.head._1
  //        val old_time = event._2.head._2
  //        // 如果此条数据已满足营销时间则更新营销时间并营销
  //        if (current_time >= (old_time.toLong + conf.getInterval * 1000)) {
  //          updateArrayBuffer.append((event._1, time_field, String.valueOf(current_time)))
  //          true
  //        } else false
  //      }
  //    }).keySet
  //    eventServer.cacheEventData(updateArrayBuffer.toArray)
  //  }

  //  /**
  //   * dataFrame格式流输出
  //   */
  //  def outputEvent(eventDF: DataFrame, uniqKeys: String) {
  //    conf.outIFIds.foreach(ifconf => {
  //      val writer = StreamWriterFactory.getWriter(ifconf)
  //      writer.push(eventDF, conf, uniqKeys)
  //    })
  //  }

  /**
   * rdd格式流输出
   */
  def outputEvent(rdd: RDD[String], uniqKeys: String) = {
    conf.outIFIds.map(ifconf => {
      val writer = StreamWriterFactory.getWriter(ifconf)
      val testtime3 = System.currentTimeMillis
      writer.push(rdd, conf, uniqKeys)
      val testtime4 = System.currentTimeMillis
    })
  }
}