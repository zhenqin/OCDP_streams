package com.asiainfo.ocdp.streaming.event

import com.asiainfo.ocdp.streaming.common.CodisCacheManager
import com.asiainfo.ocdp.streaming.config.{EventConf, MainFrameConf}
import com.asiainfo.ocdp.streaming.constant.EventConstant
import com.asiainfo.ocdp.streaming.tools.{CacheFactory, Json4sUtils, StreamWriterFactory}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer


/**
 * Created by leo on 8/12/15.
 */
class Event extends Serializable {

  var conf: EventConf = null
  var filterExpr: String = ""

  def init(eventconf: EventConf) {
    conf = eventconf
    filterExpr = conf.get("filter_expr")
  }

  def buildEvent(df: DataFrame, uniqKeys: String) {
    val eventDF = df.filter(filterExpr)
    eventDF.persist()

    if (EventConstant.NEEDCACHE == conf.get("needCache").toInt) {
      cacheEvent(eventDF, uniqKeys)
    }

    outputEvent(eventDF, uniqKeys)

  }

  val batchLimit = MainFrameConf.systemProps.getInt("cacheQryBatchSizeLimit")

  def cacheEvent(eventDF: DataFrame, uniqKeys: String) {
    val jsonRDD = eventDF.toJSON
    jsonRDD.mapPartitions({ iter => {
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
            val eventKeyValue = uniqKeys.split(",").map(current(_)).mkString(",")
            batchArrayBuffer.append((EventConstant.EVENT_CACHE_PREFIX_NAME + eventKeyValue,
              EventConstant.EVENTCACHE_FIELD_ROWEVENTID_PREFIX_KEY + conf.id, jsonValue))

            batchSize += 1
            currentPos = 0
          }

          //构建一个保存线程，提交一个批次的数据
          if (batchArrayBuffer.length > 0) {
            result = true
            CacheFactory.getManager.asInstanceOf[CodisCacheManager].setEventData(batchArrayBuffer.toArray)
          }
          result
        }
      }
    }
    })
  }


  def outputEvent(eventDF: DataFrame, uniqKeys: String) {
    conf.outIFIds.foreach(ifconf => {
      val writer = StreamWriterFactory.getWriter(ifconf)
      writer.push(eventDF, conf, uniqKeys)

    })

  }
}
