package com.asiainfo.ocdp.stream.common

import java.util

import com.asiainfo.ocdp.stream.config.MainFrameConf
import redis.clients.jedis.JedisPool

/**
  *
  *
  * <pre>
  * User: liuyu
  * Date: 2016/10/8
  * Time: 16:49
  * </pre>
  *
  * @author liuyu
  */
class LocalRouter extends Router{

	def this(cacheManager: String) {
		this()
		this.cacheManager = cacheManager
		val cacheManagers: Array[String] = this.cacheManager.split(",")
		for (manager <- cacheManagers) {
			this.hostMap.put(manager, manager)
		}
	}

	def proxyHost(host: String): util.LinkedList[JedisPool] = {

		val pool = new util.LinkedList[JedisPool]()
		val proxy = this.hostMap.filterKeys(f => {
			f.startsWith(host.trim+":")
		}).keySet
		proxy.toList.foreach(element => {
			var enabled = true  //链接是否可用的标志
			val hostAndPort:Array[String] = element.split(":")
			val jedisPool = new JedisPool(this.JedisConfig, hostAndPort(0), hostAndPort(1).toInt, MainFrameConf.systemProps.getInt("jedisTimeOut"))

			//val jedisPool =new JedisPool(JedisConfig,hostAndPort(0), hostAndPort(1).toInt, 3000)
			//通过是否抛异常判断连接是否可用
			try{
				jedisPool.getResource
			}catch {
				case _ =>{
					enabled = false
					log.error("连接不可用。。。")
				}
			}
			if(enabled) pool.add(jedisPool)
		})

		pool
	}
}
