package com.asiainfo.ocdp.streaming.event

import com.asiainfo.ocdp.streaming.config.EventConf
import com.asiainfo.ocdp.streaming.constant.EventConstant
import com.asiainfo.ocdp.streaming.tools.StreamWriterFactory
import org.apache.spark.sql.DataFrame


/**
 * Created by leo on 8/12/15.
 */
class Event extends Serializable {

  var conf: EventConf = null
  var filterExpr: String = ""

  def init(erconf: EventConf) {
    conf = erconf
    filterExpr = conf.get("filter_expr")
  }

  def buildEvent(df: DataFrame) {
    val eventDF = df.filter(filterExpr)
    eventDF.persist()

    if (EventConstant.NEEDCACHE == conf.get("needCache").toInt) {
      cacheEvent(eventDF)
    }

    outputEvent(eventDF)

  }

  def cacheEvent(eventDF: DataFrame) {}

  def outputEvent(eventDF: DataFrame) {
    conf.outIFIds.foreach(ifconf => {
      val writer = StreamWriterFactory.getWriter(ifconf)
      writer.push(eventDF, conf)

    })

  }
}
