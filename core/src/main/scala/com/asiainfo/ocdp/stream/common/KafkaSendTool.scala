package com.asiainfo.ocdp.stream.common

import java.util.Properties

import com.asiainfo.ocdp.stream.config.MainFrameConf
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

/**
 * Created by tsingfu on 15/8/18.
 */
class KafkaSendTool {
  private val currentProducer = new ThreadLocal[Producer[String, String]] {
    override def initialValue = getProducer
  }

  //TODOx: 修改配置来源 SystemProp, 目前产品架构方面存在一个缓存事件的kafka集群配置(SystemProp表中配置)，
  // 另外可以配置每个业务使用各自设置的输出kafka数据接口
  private def getProducer: Producer[String, String] = {
    val props = new Properties()
    props.put("metadata.broker.list", MainFrameConf.systemProps.get("brokerlist"))
    props.put("serializer.class", MainFrameConf.systemProps.get("serializerclass"))
    new Producer[String, String](new ProducerConfig(props))
  }

  def sendMessage(message: List[KeyedMessage[String, String]]) {
    currentProducer.get().send(message: _*)
  }

  def close() {
    currentProducer.get().close()
  }
}
