package com.asiainfo.ocdp.stream.label

import com.asiainfo.ocdp.stream.common.StreamingCache
import com.asiainfo.ocdp.stream.config.LabelConf
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types.StructType

import scala.collection.mutable
import scala.collection.mutable.Map


/**
 * Created by leo on 8/12/15.
 */
trait Label extends Serializable {

  // load config from LabelRuleConf
  var conf: LabelConf = null

  def init(lrconf: LabelConf) {
    conf = lrconf
  }

  //  def attachLabel(source: Row, cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): StreamingCache
  def attachLabel(source: GenericMutableRow, schema: StructType, cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): StreamingCache

  def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): StreamingCache

  def getQryKeys(source: GenericMutableRow, schema: StructType): Set[String] = null

  def getQryKeys(source: Map[String, String]): Set[String] = null
}
