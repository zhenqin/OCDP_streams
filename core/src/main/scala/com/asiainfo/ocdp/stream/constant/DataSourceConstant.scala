package com.asiainfo.ocdp.stream.constant

/**
 * Created by leo on 9/17/15.
 */
object DataSourceConstant {

  //For StreamingContext
  val BATCH_DURATION_SECONDS_KEY = "batch.duration.seconds"

  val KAFKA_TYPE = "kafka"
  val JDBC_TYPE = "jdbc"

  //For kafka
  val ZK_CONNECT_KEY = "zookeeper.connect"
  val BROKER_LIST_KEY = "metadata.broker.list"
  val TOPIC_KEY = "topic"
  val DELIM = ","
  val GROUP_ID_KEY = "group.id"
  val NUM_CONSUMER_FETCGERS_KEY = "num.consumer.fetchers"

  //For Hdfs
  val HDFS_DEFAULT_FS_KEY = "fs.defaultFS"
  val HDFS_PATH_KEY = "path"
}
