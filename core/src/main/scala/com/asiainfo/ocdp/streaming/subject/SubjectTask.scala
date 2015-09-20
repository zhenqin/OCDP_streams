package com.asiainfo.ocdp.streaming.subject

import com.asiainfo.ocdp.streaming.manager.StreamTask
import org.apache.spark.streaming.StreamingContext

/**
 * Created by leo on 9/16/15.
 */
class SubjectTask(id: String, interval: Int) extends StreamTask {
  def process(ssc: StreamingContext){}

}
