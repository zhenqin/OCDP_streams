package com.asiainfo.ocdp.streaming.manager

import com.asiainfo.ocdp.streaming.common.Logging
import org.apache.spark.streaming.StreamingContext

/**
 * Created by leo on 9/16/15.
 */
trait StreamTask extends Logging{

  def process(ssc: StreamingContext)

}
