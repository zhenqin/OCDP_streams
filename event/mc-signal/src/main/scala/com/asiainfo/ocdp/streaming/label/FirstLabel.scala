package com.asiainfo.ocdp.streaming.label

import com.asiainfo.ocdp.streaming.common.StreamingCache
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

/**
 * Created by tsingfu on 15/8/29.
 */
class FirstLabel extends MCLabel {
  override def attachMCLabel(mcLogRow: GenericMutableRow, schema: StructType, cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): StreamingCache = {
//  override def attachMCLabel(mcLogRow: Row, cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): StreamingCache = {

//    val mutableRow = mcLogRow.asInstanceOf[GenericMutableRow]
    val numFields = mcLogRow.length
    //增强imsi和imei标签
    val eventID = mcLogRow.getInt(0)

    // length > 14，取14个字符
    val callingimei = mcLogRow.getString(4)
    val calledimei = mcLogRow.getString(5)
    if(callingimei.length > 14) mcLogRow.setString(4, callingimei.substring(0, 14))
    if(calledimei.length > 14) mcLogRow.setString(4, calledimei.substring(0, 14))


  val callingimsi = mcLogRow.getString(6)
    val calledimsi = mcLogRow.getString(7)

    val callingphone = mcLogRow.getString(8)
    val calledphone = mcLogRow.getString(9)

    val issmsalone = mcLogRow.getInt(16)

    var imei = ""
    var imsi = ""

    if (List(3, 5, 7).contains(eventID)) {
      imei = calledimei
      imsi = calledimsi
    } else if (List(8, 9, 10, 26).contains(eventID)) {
      if (issmsalone == 1) {
        imei = callingimei
        imsi = callingimsi
      }
      else if (issmsalone == 2) {
        imei = calledimei
        imsi = calledimsi
      }
      else {
        //TODO: 解决不能实现过滤问题
//        return None
      }
    } else {
      imei = callingimei
      imsi = callingimsi
    }

    val mMap = mutable.Map[String, String]("imei" -> imei, "imsi" -> imsi)

    mcLogRow.getAs(numFields-1).asInstanceOf[mutable.Map[String, mutable.Map[String, String]]]
            .put("FirstLabel", mMap)

    cache
  }

	override def attachLabel(line: mutable.Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): StreamingCache = {
		cache
	}

}
