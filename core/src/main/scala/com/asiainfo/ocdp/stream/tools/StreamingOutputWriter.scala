package com.asiainfo.ocdp.stream.tools

import com.asiainfo.ocdp.stream.config.{SubjectConf, MainFrameConf}
import kafka.producer.KeyedMessage

import scala.collection.mutable.ArrayBuffer

/**
 * Created by tsingfu on 15/8/18.
 */
object StreamingOutputWriter extends Serializable {

  def output(outputKVsArr: Array[Array[(String, String)]], beid: String): Unit = {

    val beConf = MainFrameConf.beid2BeconfMap.get(beid).get

    //遍历每个输出数据接口
    MainFrameConf.beid2OutputDIidsMap.get(beid).get.foreach(output_diid => {
      //获取输出接口的配置
      val diConf = MainFrameConf.diid2DataInterfaceConfMap.get(output_diid).get
      val dsid = diConf.get("dsid")
      val dsConf = MainFrameConf.dsid2DataSourceConfMap.get(dsid).get
      val outputType = dsConf.get("type").toLowerCase()

      //2 如果是输出数据源类型是kafka，构造key，和批量发送记录，批量发送
      if ("kafka".equals(outputType)) {
        val topic = diConf.get("topic")

        val outputKafkaKeyMessageArr = transforEvent2KafkaMessage(outputKVsArr, beConf)

        val messages = ArrayBuffer[KeyedMessage[String, String]]()
        for ((key, msg) <- outputKafkaKeyMessageArr) {
          if (key == null) {
            messages.append(new KeyedMessage[String, String](topic, msg))
          } else {
            messages.append(new KeyedMessage[String, String](topic, key, msg))
          }
        }
        //        KafkaSendTool.sendMessage(messages.toList)
        KafkaSendTool.sendMessage(dsConf, messages.toList)


      } else if ("hdfs".equals(outputType)) {
        //      data.saveAsTextFile(conf.get("outputdir") + "/" + System.currentTimeMillis())
        throw new Exception("EventSourceType " + outputType + " is not support !")
      } else if ("jdbc".equals(outputType)) {

      } else {
        throw new Exception("EventSourceType " + outputType + " is not support !")
      }
    })

  }

  /**
   *
   * @param outputKVsArr
   * @param beConf
   * @return 返回输出到kafka的(key, message)元组的数组
   */
  def transforEvent2KafkaMessage(outputKVsArr: Array[Array[(String, String)]], beConf: SubjectConf): Array[(String, String)] = {
    val kafka_key = beConf.get("kafkakeycol", "")
    val delim = beConf.get("delim", ",")

    outputKVsArr.map(event => {
      val key = event.toMap.getOrElse(kafka_key, null)
      val outputTimeStr = DateFormatUtils.dateMs2Str(System.currentTimeMillis(), "yyyyMMdd HH:mm:ss.SSS")
      val message = (event ++ Array(("activeTimeMs", outputTimeStr))).map(_._2).mkString(delim)
      (key, message)
    })
  }
}
