package com.asiainfo.ocdp.streaming.config

/**
 * Created by tsingfu on 15/8/18.
 */
class BusinessEventConf(conf: Map[String, String] = null) extends BaseConf(conf) {
  val DEFAULT_CLASS = "com.asiainfo.ocdp.streaming.business.BusinessEvent"
  def getClassName(): String = {
    val className = get("class_name", DEFAULT_CLASS)
    if (className.nonEmpty) className else DEFAULT_CLASS
  }
}
