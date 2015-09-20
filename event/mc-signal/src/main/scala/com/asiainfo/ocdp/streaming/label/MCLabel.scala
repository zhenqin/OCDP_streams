package com.asiainfo.ocdp.streaming.label


import com.asiainfo.ocdp.streaming.common.StreamingCache
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.Map

abstract class MCLabel extends Label {

//	override def attachLabel(mcLogRow: Row, cache: StreamingCache, labelQryData: Map[String, Map[String, String]]) = mcLogRow match {
  override def attachLabel(mcLogRow: GenericMutableRow, schema: StructType, cache: StreamingCache, labelQryData: Map[String, Map[String, String]]) = mcLogRow match {
//    case so: Row => attachMCLabel(so.asInstanceOf[GenericMutableRow], cache, labelQryData)
    case so: GenericMutableRow => attachMCLabel(so, schema, cache, labelQryData)
    case _ => throw new Exception("unmatched type for log object = " + mcLogRow.getClass.getSimpleName)
  }

//  def attachMCLabel(mcLogRow: Row, cache: StreamingCache, labelQryData: Map[String, Map[String, String]]): StreamingCache
  def attachMCLabel(mcLogRow: GenericMutableRow, schema: StructType, cache: StreamingCache, labelQryData: Map[String, Map[String, String]]): StreamingCache

}