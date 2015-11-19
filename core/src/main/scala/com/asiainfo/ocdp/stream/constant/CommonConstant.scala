package com.asiainfo.ocdp.stream.constant

import java.io.File

/**
 * Created by leo on 8/12/15.
 */
object CommonConstant {
  // modify by surq at 2015.10.22 start
  //  val baseDir = ClassLoader.getSystemResource("").getFile()
  //
  //  val DBConfFile = new File(baseDir, "../conf/common.xml").getCanonicalPath()
  //
  //  val log4jConfFile = new File(baseDir, "../conf/log4j.properties").getCanonicalPath()
  //
  //  val appLogFile = new File(baseDir, "../logs/Stream_APP").getCanonicalPath()
  //
  //  val appJarsDir = new File(baseDir, "../lib").getCanonicalPath()

  val jarFilePath = CommonConstant.getClass.getProtectionDomain.getCodeSource.getLocation.getFile
  val baseDir_decode = java.net.URLDecoder.decode(jarFilePath, "UTF-8");
  val baseDir = (new File(baseDir_decode)).getParent
  val DBConfFile = new File(baseDir, "../conf/common.xml").getCanonicalPath
  val log4jConfFile = new File(baseDir, "../conf/log4j.properties").getCanonicalPath
  val appLogFile = new File(baseDir, "../logs/Stream_APP").getCanonicalPath
  val appJarsDir = new File(baseDir).getCanonicalPath
  val KafakPartitionOffsetFile = new File(baseDir, "../conf/offset.xml").getCanonicalPath()
  // modify by surq at 2015.10.22 end
}
