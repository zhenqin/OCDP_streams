package com.asiainfo.ocdp.stream.datasource

import com.asiainfo.ocdp.stream.common.Logging
import com.asiainfo.ocdp.stream.config.DataInterfaceConf
import com.asiainfo.ocdp.stream.constant.DataSourceConstant
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import scala.xml.XML
import com.asiainfo.ocdp.stream.constant.CommonConstant
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import java.io.File

object StreamingInputReader extends Logging {

  def readSource(ssc: StreamingContext, conf: DataInterfaceConf): DStream[String] = {
    val dsConf = conf.getDsConf
    val sourceType = dsConf.getDsType

    if ("kafka".equals(sourceType)) {
      val directKafkaApiFlag = conf.get("direct_kafka_api_flag", "true").toBoolean
      if (directKafkaApiFlag) {
        val topicsSet = conf.get(DataSourceConstant.TOPIC_KEY).split(DataSourceConstant.DELIM).toSet
        val kafkaParams = Map[String, String](DataSourceConstant.BROKER_LIST_KEY -> dsConf.get(DataSourceConstant.BROKER_LIST_KEY))
        logInfo("Init Direct Kafka Stream : brokers->" + dsConf.get(DataSourceConstant.BROKER_LIST_KEY) + "; topic->" + topicsSet + " ! ")
        // delete by surq at 2015.11.19 start
        //        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        //          ssc, kafkaParams, topicsSet).map(_._2)
        // delete by surq at 2015.11.19 end
        // add by surq at 2015.11.19 start
        if ((new File(CommonConstant.KafakPartitionOffsetFile)).exists) {

          val xml = XML.loadFile(CommonConstant.KafakPartitionOffsetFile)
          val partitionsNode = (xml \ "kafkapartitions")
          val partitions = (partitionsNode \ "partitions").text.split(",")
          val offsets = (partitionsNode \ "offsets").text.split(",")
          val PartitionInfo = partitions.zip(offsets).map(x => (TopicAndPartition(topicsSet.head, x._1.toInt) -> x._2.toLong)).toMap

          KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
            ssc, kafkaParams, PartitionInfo, (m: MessageAndMetadata[String, String]) => m.message())

        } else {
          KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc, kafkaParams, topicsSet).map(_._2)
        }

        // add by surq at 2015.11.19 end 

      } else {
        val zkConnect = dsConf.get(DataSourceConstant.ZK_CONNECT_KEY)
        val groupId = conf.get(DataSourceConstant.GROUP_ID_KEY)
        val numConsumerFetchers = conf.getInt(DataSourceConstant.NUM_CONSUMER_FETCGERS_KEY)

        val topicMap = Map(conf.get(DataSourceConstant.TOPIC_KEY) -> numConsumerFetchers)
        logInfo("Init Kafka Stream : zookeeper.connect->" + zkConnect + "; group.id->" + groupId + "; topic->" + topicMap + " ! ")
        KafkaUtils.createStream(ssc, zkConnect, groupId, topicMap).map(_._2)
      }

    } else if ("hdfs".equals(sourceType)) {
      val path = dsConf.get(DataSourceConstant.HDFS_DEFAULT_FS_KEY) + "/" + conf.get("path")
      ssc.textFileStream(path)

    } else {
      throw new Exception("EventSourceType " + sourceType + " is not support !")

    }
  }

}