package com.asiainfo.ocdp.streaming.manager

import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchSubmitted}

/**
 * Created by tsingfu on 15/8/18.
 */
class ReceiveRecordNumListener extends StreamingListener {
  /** Called when a batch of jobs has been submitted for processing. */
  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted) {
    //TODO: update configuration periodicly
    println("Current batch receive record number : " + batchSubmitted.batchInfo.numRecords)
  }
}
