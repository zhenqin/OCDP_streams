package com.asiainfo.ocdp.stream.config

import scala.beans.BeanProperty

/**
 * Created by leo on 8/24/15.
 */
class TaskConf() {
  @BeanProperty var id: String = ""
  @BeanProperty var task_type: Int = 0
  @BeanProperty var tid: String = ""
  @BeanProperty var name: String = ""
  @BeanProperty var receive_interval: Int = 0
  @BeanProperty var num_executors: String = ""
  @BeanProperty var executor_memory: String = ""
  @BeanProperty var total_executor_cores: String = ""
  @BeanProperty var queue: String = ""
  @BeanProperty var status: Int = 0

}
