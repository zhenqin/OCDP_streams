package com.asiainfo.ocdp.stream.manager

import java.io.File
import akka.actor.Actor
import com.asiainfo.ocdp.stream.common.Logging
import com.asiainfo.ocdp.stream.constant.CommonConstant
import scala.sys.process._

/**
 * Created by leo on 8/24/15.
 */
class Task extends Actor with Logging {

  def receive = {
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
