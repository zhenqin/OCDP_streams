package com.asiainfo.ocdp.stream.config

import scala.beans.BeanProperty

/**
 * Created by tsingfu on 15/8/18.
 */
class EventConf(conf: Map[String, String] = null) extends BaseConf(conf) {
  @BeanProperty var id: String = ""
  @BeanProperty var inIFId: String = ""
  @BeanProperty var outIFIds: Array[DataInterfaceConf] = null
  @BeanProperty var name: String = ""
  @BeanProperty var filte_expr: String = ""
  @BeanProperty var select_expr: String = ""
  @BeanProperty var p_event_id: String = ""
  @BeanProperty var interval: Int = 0
  @BeanProperty var delim: String = ""

}

