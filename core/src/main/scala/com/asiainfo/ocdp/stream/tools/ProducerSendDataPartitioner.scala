package com.asiainfo.ocdp.stream.tools

import org.slf4j.{LoggerFactory, Logger}
import kafka.producer.Partitioner

import scala.util.Random

/**
  *
  *
  * <pre>
  * User: liuyu
  * Date: 2016/10/8
  * Time: 13:56
  * </pre>
  *
  * @author liuyu
  */

class ProducerSendDataPartitioner extends Partitioner{
	val LOG: Logger = LoggerFactory.getLogger(classOf[ProducerSendDataPartitioner])

	//ToDo 自己实现分区策略，根据发送数据的key  以确保发送数据到分区的负载均衡
	override def partition(key: Any, numPartitions: Int): Int = {
		if(key == null){
			//如果发送数据的key为null,返回指定分区
			val  random = new Random()
			LOG.warn("key is null ...")
			return random.nextInt(numPartitions)

		}else{
			//如果发送数据的key不为null,根据key,实现发送数据的分区策略，以确保发送数据的负载均衡
			val result = Math.abs(key.hashCode()) % numPartitions
			//hashCode 会生成负数,所以加绝对值
			LOG.info("key is 【"+ key+ "】 partitions is 【" + result + "】 ...")
			return result

		}
	}
}
