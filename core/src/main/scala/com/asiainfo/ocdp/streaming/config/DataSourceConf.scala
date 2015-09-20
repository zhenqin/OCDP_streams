package com.asiainfo.ocdp.streaming.config

import scala.beans.BeanProperty

/**
 * Created by leo on 8/12/15.
 */
class DataSourceConf extends BaseConf {
  @BeanProperty var dsid: String = ""
  @BeanProperty var name: String = ""
  @BeanProperty var dsType: String = ""
}
