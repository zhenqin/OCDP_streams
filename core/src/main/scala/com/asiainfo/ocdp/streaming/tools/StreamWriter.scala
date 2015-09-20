package com.asiainfo.ocdp.streaming.tools

import java.util.Properties

import com.asiainfo.ocdp.streaming.config.{DataInterfaceConf, EventConf}
import kafka.producer.KeyedMessage
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, DataFrame}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by leo on 9/18/15.
 */
trait StreamWriter {
  def push(df: DataFrame, conf: EventConf)
}

class StreamKafkaWriter(diConf: DataInterfaceConf) extends StreamWriter {

  def push(df: DataFrame, conf: EventConf) {
    val jsonRDD = df.toJSON

    val topic = diConf.get("topic")

    val resultRDD: RDD[(String, String)] = transforEvent2KafkaMessage(jsonRDD, conf)

    resultRDD.mapPartitions(iter => {
      val line = iter.next()
      val key = line._1
      val msg = line._2
      val messages = ArrayBuffer[KeyedMessage[String, String]]()
      if (key == null) {
        messages.append(new KeyedMessage[String, String](topic, msg))
      } else {
        messages.append(new KeyedMessage[String, String](topic, key, msg))
      }
      KafkaSendTool.sendMessage(diConf.dsConf, messages.toList)
      iter
    }).count()

  }

  /**
   *
   * @param jsonRDD
   * @param conf
   * @return 返回输出到kafka的(key, message)元组的数组
   */
  def transforEvent2KafkaMessage(jsonRDD: RDD[String], conf: EventConf): RDD[(String, String)] = {
    val kafka_key = conf.get("kafkakeycol", "")
    val delim = conf.get("delim", ",")

    jsonRDD.map(jsonstr => {
      val data = Json4sUtils.jsonStr2Map(jsonstr)
      val key = data.getOrElse(kafka_key, null)
      (key, jsonstr)
    })
  }
}

class StreamJDBCWriter(diConf: DataInterfaceConf) extends StreamWriter {
  def push(df: DataFrame, conf: EventConf) {
    val dsConf = diConf.dsConf
    val jdbcUrl = dsConf.get("jdbcurl")
    val tableName = diConf.get("tablename")
    val properties = new Properties()
    properties.setProperty("user", dsConf.get("user"))
    properties.setProperty("password", dsConf.get("password"))
    properties.setProperty("rowId", "false")

    df.write.mode(SaveMode.Append).jdbc(jdbcUrl, tableName, properties)
  }
}