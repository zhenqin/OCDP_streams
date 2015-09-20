package com.asiainfo.ocdp.streaming.manager

import java.io.File
import akka.actor.Actor
import com.asiainfo.ocdp.streaming.common.Logging
import com.asiainfo.ocdp.streaming.constant.CommonConstant
import scala.sys.process._

/**
 * Created by leo on 8/24/15.
 */
class Task extends Actor with Logging {

  def receive = {
    /*case args: Array[String] => {
      try {
        //        SparkSubmit.main(args)
      } catch {
        // If exceptions occur after the "exit" has happened, fine to ignore them.
        // These represent code paths not reachable during normal execution.
        case e: Exception => if (!exitedCleanly) throw e
      } finally context.stop(self)
    }*/
    case cmd: (String, String) => {
      try {
        logInfo("Start task id : " + cmd._1)
        cmd._2 #>> new File(CommonConstant.appLogFile + cmd._1 + ".log") !
      } finally {
        context.stop(self)
      }
    }
  }

}
