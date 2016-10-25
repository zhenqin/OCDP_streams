package com.asiainfo.ocdp.stream.common

import java.util

import redis.clients.jedis.{JedisPool, JedisPoolConfig}


/**
  *
  *
  * <pre>
  * User: liuyu
  * Date: 2016/10/8
  * Time: 16:45
  * </pre>
  *
  * @author liuyu
  */
abstract class Router extends Logging{

	var cacheManager: String =""   //连接代理
	val hostMap = new scala.collection.mutable.HashMap[String,util.LinkedList[String]]

	/*val JedisConfig = new JedisPoolConfig()
	JedisConfig.setMaxTotal(MainFrameConf.systemProps.getInt("jedisPoolMaxTotal"))
	JedisConfig.setMaxIdle(MainFrameConf.systemProps.getInt("jedisPoolMaxIdle"))
	JedisConfig.setMinIdle(MainFrameConf.systemProps.getInt("jedisPoolMinIdle"))
	JedisConfig.setMinEvictableIdleTimeMillis(MainFrameConf.systemProps.getInt("jedisPoolMEM"))
	JedisConfig.setTestOnBorrow(true)*/

	val JedisConfig = new JedisPoolConfig()
	JedisConfig.setMaxTotal(3)
	JedisConfig.setMaxIdle(10)
	JedisConfig.setMinIdle(1)
	JedisConfig.setMinEvictableIdleTimeMillis(1)
	JedisConfig.setTestOnBorrow(true)

	def this(cacheManager: String) {
		this()
		this.cacheManager = cacheManager
		val cacheManagers: Array[String] = this.cacheManager.split(",")

		for (manager <- cacheManagers) {
			val split: Array[String] = manager.split(":")
			val maybeList = new util.LinkedList[String]()
			if(hostMap.contains(split(0))){
				maybeList.addAll(hostMap.get(split(0)).get)
			}
			maybeList.add(manager)
			hostMap.put(split(0),maybeList)
		}
	}

	def proxyHost(host: String): JedisPool
}
