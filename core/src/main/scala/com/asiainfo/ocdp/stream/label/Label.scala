package com.asiainfo.ocdp.stream.label

import com.asiainfo.ocdp.stream.common.StreamingCache
import com.asiainfo.ocdp.stream.config.LabelConf
import scala.collection.mutable


/**
 * Created by leo on 8/12/15.
 */
trait Label extends Serializable {

  // load config from LabelRuleConf
  var conf: LabelConf = null

  def init(lrconf: LabelConf) {
    conf = lrconf
  }

  def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache)

  def getQryKeys(source: Map[String, String]): Set[String] = null
}
