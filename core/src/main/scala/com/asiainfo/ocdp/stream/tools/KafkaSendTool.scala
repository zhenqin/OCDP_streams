package com.asiainfo.ocdp.stream.tools

import java.util.Properties

import com.asiainfo.ocdp.stream.config.{DataSourceConf, MainFrameConf}
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}

import scala.collection.mutable

/**
 * Created by tsingfu on 15/8/18.
 */
object KafkaSendTool {

  val DEFAULT_METADATA_BROKER_LIST = MainFrameConf.systemProps.get("metadata.broker.list", "")
  val DEFAULT_SERIALIZER_CLASS = MainFrameConf.systemProps.get("serializer.class", "kafka.serializer.StringEncoder")

  private val currentProducer = new ThreadLocal[Producer[String, String]] {
    override def initialValue = getProducer
  }

  private def getProducer: Producer[String, String] = {
    val props = new Properties()

    //TODO: 修改配置来源 SystemProp, KafkaSendTool会在两中情况下使用，
    // 1 目前产品架构方面存在一个缓存事件的kafka集群配置(SystemProp表中配置)，
    // 2 另外可以配置每个业务使用各自设置的输出kafka数据接口
    props.put("metadata.broker.list", DEFAULT_METADATA_BROKER_LIST)
    props.put("serializer.class", DEFAULT_SERIALIZER_CLASS)
    new Producer[String, String](new ProducerConfig(props))
  }

  def sendMessage(message: List[KeyedMessage[String, String]]) {
    currentProducer.get().send(message: _*)
  }

  def close() {
    currentProducer.get().close()
  }


  private val producerMap = new ThreadLocal[mutable.Map[String, Producer[String, String]]] {
    override def initialValue = getProducerMap
  }

  private def getProducerMap: mutable.Map[String, Producer[String, String]] = {
    mutable.Map[String, Producer[String, String]]()
  }

  def sendMessage(dsConf: DataSourceConf, message: List[KeyedMessage[String, String]]) {
    getProducer(dsConf).send(message: _*)
  }

  private def getProducer(dsConf: DataSourceConf): Producer[String, String] = {

    val dsid2ProducerMap = producerMap.get()
    dsid2ProducerMap.getOrElseUpdate(dsConf.dsid, {
      val props = new Properties()
      props.put("metadata.broker.list", dsConf.get("metadata.broker.list", DEFAULT_METADATA_BROKER_LIST))
      props.put("serializer.class", dsConf.get("serializer.class", DEFAULT_SERIALIZER_CLASS))
      new Producer[String, String](new ProducerConfig(props))
    })
  }

  def close(dsid: String) {
    val currentProducerMap = producerMap.get()
    if (currentProducerMap.nonEmpty) {
      currentProducerMap.get(dsid) match {
        case Some(producer) =>
          producer.close()
          currentProducerMap.remove(dsid)
        case _ =>
      }
    }

  }
}
