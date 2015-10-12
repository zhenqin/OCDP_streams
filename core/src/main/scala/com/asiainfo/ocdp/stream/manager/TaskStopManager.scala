package com.asiainfo.ocdp.stream.manager

import java.util.{Timer, TimerTask}

import com.asiainfo.ocdp.stream.common.Logging
import com.asiainfo.ocdp.stream.constant.TaskConstant
import com.asiainfo.ocdp.stream.manager.MainFrameManager._
import org.apache.spark.streaming.StreamingContext

/**
 * Created by leo on 8/30/15.
 */
class TaskStopManager(ssc: StreamingContext, taskId: String) extends Logging {

  val timer = new Timer("Task stop timer", true)
  val task = new TimerTask {
    override def run() {
      try {
        checkTaskStop(ssc, taskId)
      } catch {
        case e: Exception => logError("Error start new app ", e)
          waiter.notifyStop()
      }
    }
  }

  if (delaySeconds > 0) {
    logInfo(
      "Starting check task list status with delay of " + delaySeconds + " secs " +
        "and period of " + periodSeconds + " secs")
    timer.schedule(task, delaySeconds * 1000, periodSeconds * 1000)
  }

  def checkTaskStop(ssc: StreamingContext, id: String) {
    if (TaskConstant.PRE_STOP == taskServer.checkTaskStatus(id)) {
      ssc.stop()
//      ssc.sparkContext.stop()
    }
  }

}
