package com.asiainfo.ocdp.stream.manager

import com.asiainfo.ocdp.stream.common.Logging
import org.apache.spark.streaming.StreamingContext

/**
 * Created by leo on 9/16/15.
 */
trait StreamTask extends Logging with Serializable {

  def process(ssc: StreamingContext)

}
