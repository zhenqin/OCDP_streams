package com.asiainfo.ocdp.streaming.datasource

import com.asiainfo.ocdp.streaming.tools.{DataConvertTool, DateFormatUtils}
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

/**
 * Created by tsingfu on 15/8/29.
 */
class MCDataInterface extends DataInterface {

  override def transform(source: String, schema: StructType): Option[GenericMutableRow] = {
    val delim = conf.get("delim", ",")
    val inputs = (source + delim + "DummySplitHolder").split(delim).dropRight(1).map(_.asInstanceOf[String])

    //增强imsi和imei字段
    try {
      if (inputs(6) == "000000000000000" && inputs(7) == "000000000000000") {
        //        logError(" Emsi is wrong ! ")
        None
      } else {
        val eventID = inputs(0).toInt
        val time = inputs(1)
        val timeMs = DateFormatUtils.dateStr2Ms(time, "yyyyMMdd HH:mm:ss.SSS")

        val validWindowsTimeMs = conf.getInt("validWindowsTimeMs", -1)
        if (validWindowsTimeMs > -1 && (timeMs + validWindowsTimeMs < System.currentTimeMillis())) {
          //信令日志生成时间不在有效范围内
          None
        } else {
          //信令日志生成时间在有效范围内
          // FIXME lac ci need convert to 16 , test is 10
          /*val lac = inputs(2).toInt
        val ci = inputs(3).toInt*/
          val lac = DataConvertTool.convertHex(inputs(2))
          val ci = DataConvertTool.convertHex(inputs(3))

          var callingimei = inputs(4)
          if (callingimei.length > 14) callingimei = callingimei.substring(0, 14)

          var calledimei = inputs(5)
          if (calledimei.length > 14) calledimei = calledimei.substring(0, 14)

          val callingimsi = inputs(6)
          val calledimsi = inputs(7)
//
          val callingphone = inputs(8)
          val calledphone = inputs(9)

          val eventresult = inputs(10).toInt
          val alertstatus = inputs(11).toInt
          val assstatus = inputs(12).toInt
          val clearstatus = inputs(13).toInt
          val relstatus = inputs(14).toInt
          val xdrtype = inputs(15).toInt
          val issmsalone = inputs(16).toInt

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
              return None
            }
          } else {
            imei = callingimei
            imsi = callingimsi
          }

/*
          Some(new GenericMutableRow(Array[Any](eventID, time, lac, ci, callingimei, calledimei, callingimsi, calledimsi,
            callingphone, calledphone, eventresult, alertstatus, assstatus, clearstatus, relstatus, xdrtype, issmsalone,
            imsi, imei, mutable.Map[String, mutable.Map[String, String]]())))
*/

          val inputTuple = (eventID, time, lac, ci, callingimei, calledimei, callingimsi, calledimsi,
                  callingphone, calledphone, eventresult, alertstatus, assstatus, clearstatus, relstatus, xdrtype, issmsalone,
                  imsi, imei, mutable.Map[String, mutable.Map[String, String]]())

//          Some(new GenericMutableRow((inputs.map(_.asInstanceOf[Any]) ++ Array[Any](imsi, imei, mutable.Map[String, mutable.Map[String, String]]()))))
//          Some(new GenericMutableRow((inputs ++ Array(imsi, imei, mutable.Map[String, mutable.Map[String, String]]()))))

          Some(new GenericMutableRow(inputTuple.productIterator.toArray))

        }
      }
    } catch {
      case e: Exception =>
        //        logError(" Source columns have wrong type ! ")
        e.printStackTrace()
        None
    }
  }
}
