package com.asiainfo.ocdp.stream.manager

import java.util.{ Timer, TimerTask }
import akka.actor.{ ActorSystem, Props }
import com.asiainfo.ocdp.stream.common.Logging
import com.asiainfo.ocdp.stream.config.{ MainFrameConf, TaskConf }
import com.asiainfo.ocdp.stream.constant.{ CommonConstant, TaskConstant }
import com.asiainfo.ocdp.stream.service.TaskServer
import scala.collection.mutable

/**
 * Created by tsingfu on 15/8/26.
 */
object MainFrameManager extends Logging {
  logBegin
  val waiter = new ContextWaiter
  val delaySeconds = MainFrameConf.systemProps.getInt("delaySeconds", 10)
  val periodSeconds = MainFrameConf.systemProps.getInt("periodSeconds", 30)
  val startTimeOutSeconds = MainFrameConf.systemProps.getInt("startTimeOutSeconds", 120)

  // 对表STREAM_TASK的务服句柄
  val taskServer = new TaskServer()

  // getStatus:0 停止 1 启动中 2运行中 3停止中
  // 装载taskConf.getStatus=１的taskID和系统时间
  val pre_start_tasks = mutable.Map[String, Long]()
  // 装载taskConf.getStatus=3的taskID和系统时间
  val pre_stop_tasks = mutable.Map[String, Long]()

  val current_time = System.currentTimeMillis()
  
  
  taskServer.getAllTaskInfos().foreach(taskConf => {
    if (taskConf.getStatus == TaskConstant.PRE_START)
      pre_start_tasks.put(taskConf.getId, current_time)
  })

  //taskServer.getAllTaskInfos.filter(taskConf => taskConf.getStatus == TaskConstant.PRE_START).
  //map(taskConf => pre_start_tasks +=(taskConf.getId -> current_time))

  // Create an Akka system
  val system = ActorSystem("TaskSystem")

  private val timer = new Timer("Task build timer", true)

  private val task = new TimerTask {
    override def run() {
      try {
        buildTask()
      } catch {
        case e: Exception =>
          logError("Error start new app ", e)
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

  def main(args: Array[String]) {
    waiter.waitForStopOrError()
    timer.cancel()
    logEnd
    sys.exit()
  }

  def buildTask() {
    taskServer.getAllTaskInfos().foreach(taskConf => {
      val taskId = taskConf.getId
      taskConf.getStatus match {
        case TaskConstant.PRE_START => {
          if (pre_start_tasks.contains(taskId)) {
            val start_time = pre_start_tasks.get(taskId).get
            if (start_time + startTimeOutSeconds * 1000 >= System.currentTimeMillis()) {
              taskServer.stopTask(taskId)
              pre_start_tasks.remove(taskId)
              logInfo("Task " + taskId + " prepare start " + startTimeOutSeconds + " s has time out , stop it ! please check MainFrameManager log message !")
            }
          } else {
            // create the task actor to submit a new app and shutdown itself
            val task = system.actorOf(Props[Task], name = "task_" + taskId)
            task ! makeCMD(taskConf)
            pre_start_tasks.put(taskId, System.currentTimeMillis())
            logInfo("Task " + taskId + " prepare to start !")
          }
        }

        case TaskConstant.RUNNING => {
          if (pre_start_tasks.contains(taskId)) {
            pre_start_tasks.remove(taskId)
            logInfo("Task " + taskId + " start successfully !")
          }
        }

        case TaskConstant.PRE_STOP => {
          if (pre_stop_tasks.contains(taskId)) {
            val stop_time = pre_stop_tasks.get(taskId).get
            if (stop_time + startTimeOutSeconds * 1000 >= System.currentTimeMillis()) {
              //              taskServer.stopTask(taskId)
              pre_stop_tasks.remove(taskId)
              logInfo("Task " + taskId + " prepare stop " + startTimeOutSeconds + " s has time out , stop faile ! please check driver log message !")
            }
          } else {
            pre_stop_tasks.put(taskId, System.currentTimeMillis())
            logInfo("Task " + taskId + " prepare to stop !")
          }
        }

        case TaskConstant.STOP => {
          if (pre_stop_tasks.contains(taskId)) {
            pre_stop_tasks.remove(taskId)
            logInfo("stop Task " + taskId + "  successfully !")
          }
        }

        case _ => logInfo("No task is need operate !")
      }

    })
  }

  def makeCMD(conf: TaskConf): (String, String) = {
    val spark_home = MainFrameConf.systemProps.get("SPARK_HOME")
    val libs_dir = CommonConstant.appJarsDir
    var cmd = spark_home + "/bin/spark-submit "
    // modify by surq at 2015.10.21 start
    // val deploy_mode = " --deploy-mode cluster"
    val deploy_mode = " --deploy-mode client"

    // spark-submit files 参数追加
    var files = ""
    val files_conf = MainFrameConf.systemProps.getOption("files")
    if (files_conf != None) {
      files = " --files " + files_conf.get
    }
    // modify by surq at 2015.10.21 end

    val master = " --master " + MainFrameConf.systemProps.get("master")
    //    val jars = MainFrameConf.systemProps.get("jars")

    var jars = " --jars "
    val jars_conf = MainFrameConf.systemProps.get("jars")
    jars_conf.split(",").foreach(jar => {
      jars += libs_dir + "/" + jar + ","
    })
    jars = jars.dropRight(1)

    val appJars = libs_dir + "/" + MainFrameConf.systemProps.get("appJars")
    val streamClass = " --class com.asiainfo.ocdp.stream.manager.StreamApp"

    val executor_memory = " --executor-memory " + conf.getExecutor_memory

    val tid = conf.getId

    if (master.contains("spark")) {
      val total_executor_cores = " --total-executor-cores " + conf.getTotal_executor_cores
      var supervise = ""
      if (MainFrameConf.systemProps.get("supervise", "false").eq("true"))
        supervise = " --supervise "

      // modify by surq at 2015.10.22 start
      // cmd += streamClass + master + deploy_mode + supervise + executor_memory + total_executor_cores + jars + " " + appJars + " " + tid
      cmd += streamClass + master + deploy_mode + supervise + executor_memory + total_executor_cores + files + jars + " " + appJars + " " + tid
      // modify by surq at 2015.10.22 end
    } else if (master.contains("yarn")) {
      val num_executors = " --num-executors " + conf.getNum_executors
      var queue = " --queue "
      if (conf.getQueue != None) {
        queue += conf.getQueue
      } else {
        queue += MainFrameConf.systemProps.get("queue")
      }
      // modify by surq at 2015.10.22 start
      //cmd += streamClass + master + deploy_mode + executor_memory + num_executors + queue + jars + " " + appJars + " " + tid
      cmd += streamClass + master + deploy_mode + executor_memory + num_executors + queue + files + jars + " " + appJars + " " + tid
      // modify by surq at 2015.10.22 end
    }
    logInfo("Executor submit shell : " + cmd)
    (tid, cmd)
  }
}
