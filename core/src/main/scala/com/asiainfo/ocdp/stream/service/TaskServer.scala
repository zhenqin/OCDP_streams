package com.asiainfo.ocdp.stream.service

import com.asiainfo.ocdp.stream.common.{Logging, JDBCUtil}
import com.asiainfo.ocdp.stream.config.TaskConf
import com.asiainfo.ocdp.stream.constant.{TaskConstant, TableInfoConstant}

/**
 * Created by leo on 8/28/15.
 */
class TaskServer extends Logging {

  def startTask(id: String) {
    val sql = "update " + TableInfoConstant.TaskTableName + " set status=" + TaskConstant.RUNNING + " where id= '" + id +"'"
    JDBCUtil.execute(sql)
  }

  def stopTask(id: String) {
    val sql = "update " + TableInfoConstant.TaskTableName + " set status=" + TaskConstant.STOP + " where id='" + id +"'"
    JDBCUtil.execute(sql)
  }

  def checkTaskStatus(id: String): Int = {
    val sql = "select status from " + TableInfoConstant.TaskTableName + " where id= '" + id +"'"
    val data = JDBCUtil.query(sql)
    data.head.get("status").get.toInt
  }

  def getAllTaskInfos(): Array[TaskConf] = {

//    val a = new Array[TaskConf]()

    val sql = "select id,type,status,num_executors,executor_memory,total_executor_cores,queue from " + TableInfoConstant.TaskTableName
    val data = JDBCUtil.query(sql)
    data.map(x => {
      val taskConf = new TaskConf()
      taskConf.setId(x.get("id").get)
      taskConf.setTask_type(x.get("type").get.toInt)
      taskConf.setStatus(x.get("status").get.toInt)
      taskConf.setNum_executors(x.get("num_executors").get)
      taskConf.setExecutor_memory(x.get("executor_memory").get)
      taskConf.setTotal_executor_cores(x.get("total_executor_cores").get)
      taskConf.setQueue(x.get("queue").get)
      taskConf
    })
  }

  def getTaskInfoById(id: String): TaskConf = {
    val sql = "select id,type,tid,tname,receive_interval from " + TableInfoConstant.TaskTableName + " where id= '" + id +"'"
    val data = JDBCUtil.query(sql).head
    val taskConf = new TaskConf()
    taskConf.setId(data.get("id").get)
    taskConf.setTask_type(data.get("type").get.toInt)
    taskConf.setTid(data.get("tid").get)
    taskConf.setName(data.get("tname").get)
    taskConf.setReceive_interval(data.get("receive_interval").get.toInt)
    taskConf
  }

}
