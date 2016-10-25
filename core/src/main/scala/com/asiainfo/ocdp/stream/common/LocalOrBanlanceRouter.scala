package com.asiainfo.ocdp.stream.common

import java.util

<<<<<<< HEAD
import com.asiainfo.ocdp.stream.config.MainFrameConf
=======
>>>>>>> a0970eb026525ff329149c04c794a378b9fb70d0
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
<<<<<<< HEAD
			this.hostMap.put(manager, manager)
			this.list.add(new HostAndCouter(manager))
		}
	}

	override def proxyHost(host: String): util.LinkedList[JedisPool] = {
		val pool= new util.LinkedList[JedisPool]()
		val proxy=this.hostMap.filterKeys(f => {
			f.startsWith(host.trim+":")
		}).keySet
		proxy.toList.foreach(element => {
			var enabled = true
			val split1:Array[String] = element.split(":")
			val jedisPool = new JedisPool(this.JedisConfig, split1(0), split1(1).toInt, MainFrameConf.systemProps.getInt("jedisTimeOut"))

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

		if (pool!=null && pool.size ==0) {
			pool.addAll(super.proxyHost(host))
		}

		pool
=======
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

		/*//首先判断本机器是否存在代理
		if(this.hostMap.contains(host)){
			val hostList : util.LinkedList[String] = this.hostMap.get(host).get
			val iterator: util.Iterator[String] = hostList.iterator()
			var flag: Boolean = false
			var enabled = true  //链接是否可用的标志

			while (iterator.hasNext && !flag){
				val element: String = iterator.next()

				val hostAndPort:Array[String] = element.split(":")
				val jedis = new JedisPool(this.JedisConfig, hostAndPort(0), hostAndPort(1).toInt, MainFrameConf.systemProps.getInt("jedisTimeOut"))

				//val jedis = new JedisPool(JedisConfig,hostAndPort(0), hostAndPort(1).toInt, 3000)
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
		}*/

		if (jedisPool == null ) {
			jedisPool = super.proxyHost(host)
		}

		jedisPool
>>>>>>> a0970eb026525ff329149c04c794a378b9fb70d0
	}
}
