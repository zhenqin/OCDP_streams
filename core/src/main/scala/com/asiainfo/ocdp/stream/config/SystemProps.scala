package com.asiainfo.ocdp.stream.config

/**
 * Created by tsingfu on 15/8/19.
 */
class SystemProps extends BaseConf {
  def getInternal: Long = getLong("batchDurationS", 1)
}
