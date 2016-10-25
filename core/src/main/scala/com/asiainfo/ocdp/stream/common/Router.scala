package com.asiainfo.ocdp.stream.common

import java.util

<<<<<<< HEAD
import com.asiainfo.ocdp.stream.config.MainFrameConf
=======
>>>>>>> a0970eb026525ff329149c04c794a378b9fb70d0
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
<<<<<<< HEAD

	var cacheManager: String=""   //连接代理
	val hostMap= new scala.collection.mutable.HashMap[String,String]
=======

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
>>>>>>> a0970eb026525ff329149c04c794a378b9fb70d0

	val JedisConfig = new JedisPoolConfig()
	JedisConfig.setMaxTotal(MainFrameConf.systemProps.getInt("jedisPoolMaxTotal"))
	JedisConfig.setMaxIdle(MainFrameConf.systemProps.getInt("jedisPoolMaxIdle"))
	JedisConfig.setMinIdle(MainFrameConf.systemProps.getInt("jedisPoolMinIdle"))
	JedisConfig.setMinEvictableIdleTimeMillis(MainFrameConf.systemProps.getInt("jedisPoolMEM"))
	JedisConfig.setTestOnBorrow(true)

	/*val JedisConfig = new JedisPoolConfig()
	JedisConfig.setMaxTotal(3)
	JedisConfig.setMaxIdle(10)
	JedisConfig.setMinIdle(1)
	JedisConfig.setMinEvictableIdleTimeMillis(1)
	JedisConfig.setTestOnBorrow(true)*/

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

<<<<<<< HEAD
	def proxyHost(host: String): util.LinkedList[JedisPool]
=======
	def proxyHost(host: String): JedisPool
>>>>>>> a0970eb026525ff329149c04c794a378b9fb70d0
}
