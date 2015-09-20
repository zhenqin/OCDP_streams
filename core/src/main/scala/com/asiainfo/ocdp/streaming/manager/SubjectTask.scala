package com.asiainfo.ocdp.streaming.manager

import org.apache.spark.streaming.StreamingContext

/**
 * Created by leo on 9/16/15.
 */
class SubjectTask(id: String, interval: Int) extends StreamTask {
  def process(ssc: StreamingContext){}

}
