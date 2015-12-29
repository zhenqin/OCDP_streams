package com.asiainfo.ocdp.stream.tools

import java.util.Properties
import com.asiainfo.ocdp.stream.config.{ DataInterfaceConf, EventConf }
import kafka.producer.KeyedMessage
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SaveMode }
import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat

/**
 * Created by surq on 12/09/15
 */
trait StreamWriter extends Serializable {
  def push(df: DataFrame, conf: EventConf, uniqKeys: String)
  def push(rdd: RDD[String], conf: EventConf, uniqKeys: String)
}

/**
 * Created by surq on 12/09/15
 */
class StreamKafkaWriter(diConf: DataInterfaceConf) extends StreamWriter {

  //  def push(df: DataFrame, conf: EventConf, uniqKeys: String) {
  //    val jsonRDD = df.toJSON
  //    val topic = diConf.get("topic")
  //    var fildList = conf.select_expr.split(",")
  //    // add by surq at 2015.11.21 start 
  //    if (conf.get("ext_fields", null) != null && conf.get("ext_fields") != "") {
  //      val fields = conf.get("ext_fields").split(",").map(ext => (ext.split("as"))(1).trim)
  //      fildList = fildList ++ fields
  //    }
  //    // add by surq at 2015.11.21 end   
  //
  //    val delim = conf.delim
  //    val resultRDD: RDD[(String, String)] = transforEvent2KafkaMessage(jsonRDD, uniqKeys)
  //    // modify by surq at 2015.11.03 start
  //    //    resultRDD.mapPartitions(iter => {
  //    //      if (iter.hasNext) {
  //    //        val line = iter.next()
  //    //        val key = line._1
  //    //        val msg = line._2
  //    //        val messages = ArrayBuffer[KeyedMessage[String, String]]()
  //    //        if (key == null) {
  //    //          messages.append(new KeyedMessage[String, String](topic, msg))
  //    //        } else {
  //    //          messages.append(new KeyedMessage[String, String](topic, key, msg))
  //    //        }
  //    //        KafkaSendTool.sendMessage(diConf.dsConf, messages.toList)
  //    //      }
  //    //      iter
  //    //    }).count()
  //    val cont = resultRDD.mapPartitions(iter => {
  //      iter.toList.map(line =>
  //        {
  //          val key = line._1
  //          val msg_json = line._2
  //          val msg = Json4sUtils.jsonStr2String(msg_json, fildList, delim)
  //          val messages = ArrayBuffer[KeyedMessage[String, String]]()
  //          if (key == null) {
  //            messages.append(new KeyedMessage[String, String](topic, msg))
  //          } else {
  //            messages.append(new KeyedMessage[String, String](topic, key, msg))
  //          }
  //          KafkaSendTool.sendMessage(diConf.dsConf, messages.toList)
  //          key
  //        }).toIterator
  //    }).count()
  //    // modify by surq at 2015.11.03 end
  //  }

  def push(rdd: RDD[String], conf: EventConf, uniqKeys: String)= setMessage(rdd, conf, uniqKeys).count
  def push(df: DataFrame, conf: EventConf, uniqKeys: String) = setMessage(df.toJSON, conf, uniqKeys).count

  
  // 向kafka发送数据的未尾字段加当前时间
  val dateFormat = "yyyyMMdd HH:mm:ss.SSS"
  val sdf =  new SimpleDateFormat(dateFormat)
  /**
   * 向kafka发送数据
   */
  
  def setMessage(jsonRDD: RDD[String], conf: EventConf, uniqKeys: String) = {
    
    var fildList = conf.select_expr.split(",")
    if (conf.get("ext_fields", null) != null && conf.get("ext_fields") != "") {
      val fields = conf.get("ext_fields").split(",").map(ext => (ext.split("as"))(1).trim)
      fildList = fildList ++ fields
    }
    val delim = conf.delim
    val topic = diConf.get("topic")

    val resultRDD: RDD[(String, String)] = transforEvent2KafkaMessage(jsonRDD, uniqKeys)
    resultRDD.mapPartitions(iter => {
      val messages = ArrayBuffer[KeyedMessage[String, String]]()
      val it = iter.toList.map(line =>
        {
          val key = line._1
          val msg_json = line._2
          val msg_head = Json4sUtils.jsonStr2String(msg_json, fildList, delim)
          // 加入当前msg输出时间戳
          val msg = msg_head + delim + sdf.format(System.currentTimeMillis)
          if (key == null) messages.append(new KeyedMessage[String, String](topic, msg))
          else messages.append(new KeyedMessage[String, String](topic, key, msg))
          key
        })
      val msgList = messages.toList
      if (msgList.size > 0) KafkaSendTool.sendMessage(diConf.dsConf, msgList)
      it.iterator
    })
  }

  /**
   *
   * @param jsonRDD
   * @param uniqKeys
   * @return 返回输出到kafka的(key, message)元组的数组
   */
  def transforEvent2KafkaMessage(jsonRDD: RDD[String], uniqKeys: String): RDD[(String, String)] = {

    jsonRDD.map(jsonstr => {
      val data = Json4sUtils.jsonStr2Map(jsonstr)
      val key = uniqKeys.split(",").map(data(_)).mkString(",")
      (key, jsonstr)
    })
  }
}

class StreamJDBCWriter(diConf: DataInterfaceConf) extends StreamWriter {
  def push(df: DataFrame, conf: EventConf, uniqKeys: String) {
    val dsConf = diConf.dsConf
    val jdbcUrl = dsConf.get("jdbcurl")
    val tableName = diConf.get("tablename")
    val properties = new Properties()
    properties.setProperty("user", dsConf.get("user"))
    properties.setProperty("password", dsConf.get("password"))
    properties.setProperty("rowId", "false")

    df.write.mode(SaveMode.Append).jdbc(jdbcUrl, tableName, properties)
  }

  def push(rdd: RDD[String], conf: EventConf, uniqKeys: String) = {}
}