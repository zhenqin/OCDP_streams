package com.asiainfo.ocdp.stream.common

import java.util

<<<<<<< HEAD
import com.asiainfo.ocdp.stream.config.MainFrameConf
import redis.clients.jedis.JedisPool

=======
import redis.clients.jedis.JedisPool


>>>>>>> a0970eb026525ff329149c04c794a378b9fb70d0
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
<<<<<<< HEAD
class LocalRouter extends Router{

	def this(cacheManager: String) {
		this()
		this.cacheManager = cacheManager
		val cacheManagers: Array[String] = this.cacheManager.split(",")
		for (manager <- cacheManagers) {
			this.hostMap.put(manager, manager)
=======
class LocalRouter(cacheManager: String) extends Router(cacheManager){

	def proxyHost(host: String): JedisPool = {

		var jedisPool: JedisPool = null
		//首先判断本机器是否存在代理
		if(this.hostMap.contains(host)){
			val hostList : util.LinkedList[String] = this.hostMap.get(host).get

			val iterator: util.Iterator[String] = hostList.iterator()
			var flag: Boolean = false
			var enabled = true  //链接是否可用的标志

			while (iterator.hasNext && !flag){
				val element: String = iterator.next()

				val hostAndPort:Array[String] = element.split(":")
				//val jedis= new JedisPool(this.JedisConfig, hostAndPort(0), hostAndPort(1).toInt, MainFrameConf.systemProps.getInt("jedisTimeOut"))

				val jedis =new JedisPool(JedisConfig,hostAndPort(0), hostAndPort(1).toInt, 3000)
				//通过是否抛异常判断连接是否可用
				try{
					jedis.getResource
				}catch {
					case _ =>{
						enabled = false
						log.error("连接不可用。。。")
					}
				}

				if(enabled) {
					jedisPool = jedis
					flag = true
				}
			}
>>>>>>> a0970eb026525ff329149c04c794a378b9fb70d0
		}

<<<<<<< HEAD
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
=======
		jedisPool
>>>>>>> a0970eb026525ff329149c04c794a378b9fb70d0
	}
}
