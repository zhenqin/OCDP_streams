package com.asiainfo.ocdp.stream.tools

import java.util.Properties

import com.asiainfo.ocdp.stream.config.{ DataSourceConf, MainFrameConf }
import kafka.producer.{ KeyedMessage, ProducerConfig, Producer }

import scala.collection.mutable

/**
 * Created by tsingfu on 15/8/18.
 */
//object KafkaSendTool {
//
//  val DEFAULT_METADATA_BROKER_LIST = MainFrameConf.systemProps.get("metadata.broker.list", "")
//  val DEFAULT_SERIALIZER_CLASS = MainFrameConf.systemProps.get("serializer.class", "kafka.serializer.StringEncoder")
//
//  private val currentProducer = new ThreadLocal[Producer[String, String]] {
//    override def initialValue = getProducer
//  }
//
//  private def getProducer: Producer[String, String] = {
//    val props = new Properties()
//
//    //TODO: 修改配置来源 SystemProp, KafkaSendTool会在两中情况下使用，
//    // 1 目前产品架构方面存在一个缓存事件的kafka集群配置(SystemProp表中配置)，
//    // 2 另外可以配置每个业务使用各自设置的输出kafka数据接口
//    props.put("metadata.broker.list", DEFAULT_METADATA_BROKER_LIST)
//    props.put("serializer.class", DEFAULT_SERIALIZER_CLASS)
//    new Producer[String, String](new ProducerConfig(props))
//  }
//
//  def sendMessage(message: List[KeyedMessage[String, String]]) {
//    currentProducer.get().send(message: _*)
//  }
//
//  def close() {
//    currentProducer.get().close()
//  }
//
//  private val producerMap = new ThreadLocal[mutable.Map[String, Producer[String, String]]] {
//    override def initialValue = getProducerMap
//  }
//
//  private def getProducerMap: mutable.Map[String, Producer[String, String]] = {
//    mutable.Map[String, Producer[String, String]]()
//  }
//
//  // modified by surq at 2015.12.10 ---start-----
//  //  def sendMessage(dsConf: DataSourceConf, message: List[KeyedMessage[String, String]]) {
//  //    getProducer(dsConf).send(message: _*)
//  //  }
//// // 分包发送
////  def sendMessage(dsConf: DataSourceConf, message: List[KeyedMessage[String, String]]) {
////    val producer = getProducer(dsConf)
////    val msgList = message.sliding(200, 200)
////    msgList.foreach(list => producer.send(list: _*))
////  }
//
//  // 多线程、多producer,分包发送
//  def sendMessage(dsConf: DataSourceConf, message: List[KeyedMessage[String, String]]) =
//    message.sliding(200, 200).foreach(list => CacheQryThreadPool.threadPool.execute(new Runnable {
//      override def run() = getProducer(dsConf).send(list: _*)
//    }))
//  // modified by surq at 2015.12.10 ---end-----
//  private def getProducer(dsConf: DataSourceConf): Producer[String, String] = {
//
//    val dsid2ProducerMap = producerMap.get()
//    dsid2ProducerMap.getOrElseUpdate(dsConf.dsid, {
//      val props = new Properties()
//      props.put("metadata.broker.list", dsConf.get("metadata.broker.list", DEFAULT_METADATA_BROKER_LIST))
//      props.put("serializer.class", dsConf.get("serializer.class", DEFAULT_SERIALIZER_CLASS))
//      new Producer[String, String](new ProducerConfig(props))
//    })
//  }
//
//  def close(dsid: String) {
//    val currentProducerMap = producerMap.get()
//    if (currentProducerMap.nonEmpty) {
//      currentProducerMap.get(dsid) match {
//        case Some(producer) =>
//          producer.close()
//          currentProducerMap.remove(dsid)
//        case _ =>
//      }
//    }
//
//  }
//}

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