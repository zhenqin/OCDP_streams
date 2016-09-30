package com.asiainfo.ocdp.stream.manager

import com.asiainfo.ocdp.stream.config.{DataInterfaceConf, TaskConf}
import com.asiainfo.ocdp.stream.constant.TaskConstant
import com.asiainfo.ocdp.stream.datasource.DataInterfaceTask
import com.asiainfo.ocdp.stream.event.Event
import com.asiainfo.ocdp.stream.label.Label
import com.asiainfo.ocdp.stream.service.DataInterfaceServer
import com.asiainfo.ocdp.stream.subject.SubjectTask

/**
  * Created by leo on 9/16/15.
  */
object StreamTaskFactory {

  def getStreamTask(taskConf: TaskConf): StreamTask = {
    val tid = taskConf.getTid
    val interval = taskConf.getReceive_interval
    val taskType = taskConf.getTask_type
    if (TaskConstant.TYPE_DATAINTERFACE == taskType) {

      val dataInterfaceService = new DataInterfaceServer

      val conf: DataInterfaceConf = dataInterfaceService.getDataInterfaceInfoById(tid)
      val labels: Array[Label] = dataInterfaceService.getLabelsByIFId(tid)
      val events: Array[Event] = dataInterfaceService.getEventsByIFId(tid)
      new DataInterfaceTask(tid, interval, conf, labels, events)
    }
    else if (TaskConstant.TYPE_SUTJECT == taskType)
      new SubjectTask(tid, interval)
    else throw new Exception("Task type " + taskType + " is not supported !")
  }

}
