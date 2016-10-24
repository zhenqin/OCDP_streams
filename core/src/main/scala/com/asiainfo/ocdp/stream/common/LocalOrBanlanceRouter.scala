package com.asiainfo.ocdp.stream.common

import java.util

import com.asiainfo.ocdp.stream.config.MainFrameConf
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
class LocalOrBanlanceRouter  extends BanlanceRouter{

	def this(cacheManager: String) {
		this()
		this.cacheManager=cacheManager
		val cacheManagers: Array[String] = this.cacheManager.split(",")
		for (manager <- cacheManagers) {
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
	}
}
