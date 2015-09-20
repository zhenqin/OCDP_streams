package com.asiainfo.ocdp.streaming.manager

import com.asiainfo.ocdp.streaming.config.TaskConf
import com.asiainfo.ocdp.streaming.constant.TaskConstant

/**
 * Created by leo on 9/16/15.
 */
object StreamTaskFactory {

  def getStreamTask(taskConf: TaskConf): StreamTask = {
    val tid = taskConf.getTid
    val interval = taskConf.getReceive_interval
    val taskType = taskConf.getTask_type
    if (TaskConstant.TYPE_DATAINTERFACE == taskType)
      new DataInterfaceTask(tid, interval)
    else if (TaskConstant.TYPE_SUTJECT == taskType)
      new SubjectTask(tid, interval)
    else throw new Exception("Task type " + taskType + " is not supported !")
  }

}
