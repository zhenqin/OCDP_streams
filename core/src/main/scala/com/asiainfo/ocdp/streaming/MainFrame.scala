/*
package com.asiainfo.ocdp.streaming

import com.asiainfo.ocdp.streaming.business.BusinessEvent
import com.asiainfo.ocdp.streaming.config.{DataInterfaceConf, LabelConf, MainFrameConf}
import com.asiainfo.ocdp.streaming.datasource.{StreamingInputReader, DataInterface}
import com.asiainfo.ocdp.streaming.label.Label
import com.asiainfo.ocdp.streaming.manager.ReceiveRecordNumListener
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * Created by leo on 8/12/15.
 */
object MainFrame {

  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println("usage:  ./bin/spark-class com.asiainfo.ocdc.streaming.mainframe [master] [appname] [datainterfaceid]")
      System.exit(1)
    }

    val master = args(0)
    val appname = args(1)
    val dataifid = args(2)

    //1 初始化 streamingContext
    val sparkConf = new SparkConf().setMaster(master).setAppName(appname)
    val interval = MainFrameConf.diid2DataInterfaceConfMap.get(dataifid).get.get(StreamingInputReader.BATCH_DURATION_SECONDS_KEY).toInt
    val ssc = new StreamingContext(sparkConf, Seconds(interval))
    ssc.addStreamingListener(new ReceiveRecordNumListener())

    //2 根据配置初始化处理流数据的标签增强类，事件过滤类实例
    initDataInterface(dataifid).process(ssc)

    //4 启动 streamingContext
    ssc.start()
    ssc.awaitTermination()
    exit()
  }

  def initDataInterface(diid: String): DataInterface = {
    val dataInterfaceConf: DataInterfaceConf = MainFrameConf.diid2DataInterfaceConfMap.get(diid).get
    val dataInterface = Class.forName(dataInterfaceConf.get("class_name")).newInstance().asInstanceOf[DataInterface]
    //    val dataInterface = Class.forName("com.asiainfo.ocdp.streaming.datasource.DataInterface").newInstance().asInstanceOf[DataInterface]
    dataInterface.init(dataInterfaceConf)

    MainFrameConf.diid2LabelId2LabelRuleConfMap.get(diid).getOrElse(mutable.Map[String, LabelConf]()).values.map(labelRuleConf => {
      val labelRule: Label =
        Class.forName(labelRuleConf.getClass_name).newInstance().asInstanceOf[Label]
      labelRule.init(labelRuleConf)
      dataInterface.addLabelRule(labelRule)
    })

    //    MainFrameConf.diid2EventId2EventRuleConfMap.get(diid).getOrElse(mutable.Map[String, EventRuleConf]()).values.map(eventRuleConf => {
    //      val eventRule: EventRule =
    //        Class.forName(eventRuleConf.getClassName()).newInstance().asInstanceOf[EventRule]
    //      eventRule.init(eventRuleConf)
    //      dataInterface.addEventRule(eventRule)
    //    })

    MainFrameConf.diid2BeidsMap.get(diid).get.map(beid => {
      val beConf = MainFrameConf.beid2BeconfMap.get(beid).get
      val bsEvent: BusinessEvent =
        Class.forName(beConf.getClassName()).newInstance().asInstanceOf[BusinessEvent]
      bsEvent.init(dataInterface.id, beConf)
      dataInterface.addBsEvent(bsEvent)
    })

    dataInterface
  }
}
*/
