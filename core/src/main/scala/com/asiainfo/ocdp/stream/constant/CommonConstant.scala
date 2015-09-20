package com.asiainfo.ocdp.stream.constant

import java.io.File

/**
 * Created by leo on 8/12/15.
 */
object CommonConstant {

  val baseDir = ClassLoader.getSystemResource("").getFile()

  val DBConfFile = new File(baseDir, "../conf/common.xml").getCanonicalPath()

  val log4jConfFile = new File(baseDir, "../conf/log4j.properties").getCanonicalPath()

  val appLogFile = new File(baseDir, "../logs/Stream_APP").getCanonicalPath()

  val appJarsDir = new File(baseDir, "../lib").getCanonicalPath()
}
