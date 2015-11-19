package com.asiainfo.ocdp.stream.event

import com.asiainfo.ocdp.stream.config.{ EventConf, MainFrameConf }
import com.asiainfo.ocdp.stream.constant.EventConstant
import com.asiainfo.ocdp.stream.service.EventServer
import com.asiainfo.ocdp.stream.tools.{ Json4sUtils, StreamWriterFactory }
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD

/**
 * Created by leo on 8/12/15.
 */
class Event extends Serializable {

  var conf: EventConf = null

  def init(eventconf: EventConf) {
    conf = eventconf
  }

  val eventServer = new EventServer()

  def buildEvent(df: DataFrame, uniqKeys: String) {

    var mix_sel_expr = uniqKeys.split(":") ++ conf.select_expr.split(",")
    if (conf.get("ext_fields", null) != null)
      mix_sel_expr = mix_sel_expr ++ conf.get("ext_fields", null).split(",")

    var eventDF = df.filter(conf.filte_expr).selectExpr(mix_sel_expr: _*)
    var jsonRDD: RDD[String] = null

    //    eventDF.persist()

    if (EventConstant.NEEDCACHE == conf.getInt("needcache", 0)) {
      cacheEvent(eventDF, uniqKeys)
    }

    if (EventConstant.RealtimeTransmission != conf.interval) {
      val tuple = checkEvent(eventDF, uniqKeys)
      eventDF = tuple._1
      jsonRDD = tuple._2
    }

    outputEvent(eventDF, uniqKeys)

    if (jsonRDD != null)
      jsonRDD.unpersist()

    //    eventDF.unpersist()
  }

  val batchLimit = MainFrameConf.systemProps.getInt("cacheQryBatchSizeLimit")

  def cacheEvent(eventDF: DataFrame, uniqKeys: String) {
    val jsonRDD = eventDF.toJSON
    jsonRDD.mapPartitions({ iter =>
      {
        new Iterator[String] {
          private[this] var currentPos: Int = -1
          private[this] val batchArrayBuffer = new ArrayBuffer[(String, String, String)]()

          override def hasNext: Boolean = (currentPos != -1 && currentPos < batchArrayBuffer.length) || (iter.hasNext && batchNext())

          override def next(): String = {
            currentPos += 1
            batchArrayBuffer(currentPos - 1)._3
          }

          //批量处理
          def batchNext(): Boolean = {
            currentPos = -1
            var batchSize = 0
            var result = false

            batchArrayBuffer.clear()

            //获取一个批次处理的row
            while (iter.hasNext && (batchSize < batchLimit)) {
              val jsonValue = iter.next()
              val current = Json4sUtils.jsonStr2Map(jsonValue)
              val eventKeyValue = uniqKeys.split(":").map(current(_)).mkString(":")
              batchArrayBuffer.append((EventConstant.EVENT_CACHE_PREFIX_NAME + eventKeyValue,
                EventConstant.EVENTCACHE_FIELD_ROWEVENTID_PREFIX_KEY + conf.id, jsonValue))

              batchSize += 1
            }

            //构建一个保存线程，提交一个批次的数据
            if (batchArrayBuffer.length > 0) {
              result = true
              currentPos = 0
              eventServer.cacheEventData(batchArrayBuffer.toArray)
            }
            result
          }
        }
      }
    }).count()
  }

  def checkEvent(eventDF: DataFrame, uniqKeys: String): (DataFrame, RDD[String]) = {
    var jsonRDD = eventDF.toJSON
    jsonRDD = jsonRDD.mapPartitions(iter => {
      new Iterator[String] {
        private[this] var currentPos: Int = -1
        private[this] var resultBuffer = new ArrayBuffer[(String, String)]()

        override def hasNext: Boolean = (currentPos != -1 && currentPos < resultBuffer.length) || (iter.hasNext && batchNext())

        override def next(): String = {
          currentPos += 1
          resultBuffer(currentPos - 1)._2
        }

        //批量处理
        def batchNext(): Boolean = {
          currentPos = -1
          var batchSize = 0
          resultBuffer.clear()
          val batchArrayBuffer = new ArrayBuffer[(String, Array[String])]()

          //获取一个批次处理的row
          while (iter.hasNext && (batchSize < batchLimit)) {
            val jsonValue = iter.next()
            val current = Json4sUtils.jsonStr2Map(jsonValue)
            val eventKeyValue = uniqKeys.split(":").map(current(_)).mkString(":")
            batchArrayBuffer.append((EventConstant.EVENT_CACHE_PREFIX_NAME + eventKeyValue,
              Array(EventConstant.EVENTCACHE_FIELD_TIMEEVENTID_PREFIX_KEY + conf.id)))
            resultBuffer.append((EventConstant.EVENT_CACHE_PREFIX_NAME + eventKeyValue, jsonValue))
            batchSize += 1
          }

          //构建一个保存线程，提交一个批次的数据
          if (batchArrayBuffer.length > 0) {
            val eventTimes = eventServer.getEventCache(batchArrayBuffer.toArray)

            val updateArrayBuffer = new ArrayBuffer[(String, String, String)]()
            val current_time = System.currentTimeMillis()
            val outputKeys = eventTimes.filter(event => {

              if (event._2.size == 0) {
                updateArrayBuffer.append((event._1, EventConstant.EVENTCACHE_FIELD_TIMEEVENTID_PREFIX_KEY + conf.id, String.valueOf(current_time)))
                true
              } else {
                val time_field = event._2.head._1
                val old_time = event._2.head._2
                if (current_time >= (old_time.toLong + conf.getInterval * 1000)) {
                  updateArrayBuffer.append((event._1, time_field, String.valueOf(current_time)))
                  true
                } else false
              }
            }).keySet

            eventServer.cacheEventData(updateArrayBuffer.toArray)

            resultBuffer = resultBuffer.dropWhile(x => !outputKeys.contains(x._1))

          }

          if (resultBuffer.length > 0) {
            currentPos = 0
            true
          } else false

        }
      }
    })

    jsonRDD.persist()
    (eventDF.sqlContext.read.json(jsonRDD), jsonRDD)
  }

  def outputEvent(eventDF: DataFrame, uniqKeys: String) {
    conf.outIFIds.foreach(ifconf => {
      val writer = StreamWriterFactory.getWriter(ifconf)
      writer.push(eventDF, conf, uniqKeys)

    })

  }
}
