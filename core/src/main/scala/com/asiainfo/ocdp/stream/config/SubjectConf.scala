package com.asiainfo.ocdp.stream.config

import scala.beans.BeanProperty

/**
 * Created by tsingfu on 15/8/18.
 */
class SubjectConf(conf: Map[String, String] = null) extends BaseConf(conf) {
  @BeanProperty var id: String = ""
  @BeanProperty var name: String = ""
  @BeanProperty var events: Map[String, String] = null
  @BeanProperty var status: Int = 0
}
