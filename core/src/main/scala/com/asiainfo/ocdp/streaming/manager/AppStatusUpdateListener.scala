package com.asiainfo.ocdp.streaming.manager

import com.asiainfo.ocdp.streaming.common.Logging
import com.asiainfo.ocdp.streaming.service.TaskServer
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart}

/**
 * Created by leo on 9/1/15.
 */
class AppStatusUpdateListener(id: String) extends SparkListener with Logging {
  val taskServer = new TaskServer

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart) {
    taskServer.startTask(id)
    logInfo("Start task " + id + " sucess2 !")
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
    taskServer.stopTask(id)
    logInfo("Stop task " + id + " sucess2 !")
  }

}
