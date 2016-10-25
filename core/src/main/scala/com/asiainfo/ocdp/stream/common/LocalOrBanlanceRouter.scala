package com.asiainfo.ocdp.stream.common

import java.util

import redis.clients.jedis.JedisPool

/**
  *
  *
  * <pre>
  * User: liuyu
  * Date: 2016/10/9
  * Time: 10:31
  * </pre>
  *
  * @author liuyu
  */
class LocalOrBanlanceRouter  extends BanlanceRouter {

	def this(cacheManager: String) {
		this()
		this.cacheManager=cacheManager
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

	override def proxyHost(host: String): JedisPool = {
		var jedisPool:JedisPool = null

		jedisPool = new LocalRouter(cacheManager).proxyHost(host)

		if (jedisPool == null ) {
			jedisPool = super.proxyHost(host)
		}

		jedisPool
	}
}
