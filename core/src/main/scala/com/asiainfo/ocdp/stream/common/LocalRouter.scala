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
			val split: Array[String] = manager.split(":")
			val maybeList = new util.LinkedList[String]()
			if(hostMap.contains(split(0))){
				maybeList.addAll(hostMap.get(split(0)).get)
			}
			maybeList.add(manager)
			hostMap.put(split(0),maybeList)
		}
	}

	def proxyHost(host: String): JedisPool = {

		var jedisPool: JedisPool = null
		//首先判断本机器是否存在代理
		if(this.hostMap.contains(host)){
			val hostList : util.LinkedList[String] = this.hostMap.get(host).get

			val iterator: util.Iterator[String] = hostList.iterator()
			while (iterator.hasNext){
				val element: String = iterator.next()
				var enabled = true  //链接是否可用的标志
				val hostAndPort:Array[String] = element.split(":")
				val jedis= new JedisPool(this.JedisConfig, hostAndPort(0), hostAndPort(1).toInt, MainFrameConf.systemProps.getInt("jedisTimeOut"))

				//val jedisPool =new JedisPool(JedisConfig,hostAndPort(0), hostAndPort(1).toInt, 3000)
				//通过是否抛异常判断连接是否可用
				try{
					jedis.getResource
				}catch {
					case _ =>{
						enabled = false
						log.error("连接不可用。。。")
					}
				}

				if(enabled) jedisPool = jedis
			}
		}

		jedisPool
	}
}
