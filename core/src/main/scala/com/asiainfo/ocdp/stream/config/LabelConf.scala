package com.asiainfo.ocdp.stream.config

import scala.beans.BeanProperty

/**
 * Created by tsingfu on 15/8/18.
 */
class LabelConf(conf: Map[String, String] = null) extends BaseConf(conf) {
  @BeanProperty var id: String = ""
  @BeanProperty var diid: String = ""
  @BeanProperty var name: String = ""
  @BeanProperty var class_name: String = ""
  @BeanProperty var plabelId: String = ""
  @BeanProperty var status: Int = 0
  @BeanProperty var fields: List[String] = null
}
