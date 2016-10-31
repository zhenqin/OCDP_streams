package com.asiainfo.ocdp.stream.tools

import java.util.Properties

import com.asiainfo.ocdp.stream.config.{ DataSourceConf, MainFrameConf }
import kafka.producer.{ KeyedMessage, ProducerConfig, Producer }

import scala.collection.mutable

/**
 * Created by tsingfu on 15/8/18.
 */

// -------update by surq start 2015.12.14 ---------------------------------

object KafkaSendTool {

  val DEFAULT_METADATA_BROKER_LIST = MainFrameConf.systemProps.get("metadata.broker.list", "")
  val DEFAULT_SERIALIZER_CLASS = MainFrameConf.systemProps.get("serializer.class", "kafka.serializer.StringEncoder")

  // surq:一个datasource 对应一个producer，跟业务个数无直接关系
  val dsid2ProducerMap = mutable.Map[String, Producer[String, String]]()

  // 多线程、多producer,分包发送
  //  def sendMessage(dsConf: DataSourceConf, message: List[KeyedMessage[String, String]]) =
  //    message.sliding(200, 200).foreach(list => CacheQryThreadPool.threadPool.execute(new Runnable {
  //      override def run() = getProducer(dsConf).send(list: _*)
  //    }))

  def sendMessage(dsConf: DataSourceConf, message: List[KeyedMessage[String, String]]) = {
    val msgList = message.sliding(200, 200)
    if (msgList.size > 0){
    	val producer = getProducer(dsConf)
    	message.sliding(200, 200).foreach(list => producer.send(list: _*))
    }
  }

  // 对应的producer若不存在，则创建新的producer，并存入dsid2ProducerMap
  private def getProducer(dsConf: DataSourceConf): Producer[String, String] =
    dsid2ProducerMap.getOrElseUpdate(dsConf.dsid, {
      val props = new Properties()
      props.put("metadata.broker.list", dsConf.get("metadata.broker.list", DEFAULT_METADATA_BROKER_LIST))
      props.put("serializer.class", dsConf.get("serializer.class", DEFAULT_SERIALIZER_CLASS))
      new Producer[String, String](new ProducerConfig(props))
    })

}