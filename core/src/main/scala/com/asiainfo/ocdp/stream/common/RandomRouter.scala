package com.asiainfo.ocdp.stream.common

import java.util
import java.util.Random

import com.asiainfo.ocdp.stream.config.MainFrameConf
import redis.clients.jedis.JedisPool

/**
  *
  *
  * <pre>
  * User: liuyu
  * Date: 2016/10/8
  * Time: 16:46
  * </pre>
  *
  * @author liuyu
  */
class RandomRouter extends Router {

	def this(cacheManager: String) {
		this()
		this.cacheManager = cacheManager
	}

	def proxyHost(host: String): util.LinkedList[JedisPool] = {
		val split: Array[String] = cacheManager.split(",")
		val flag: Boolean = false
		val codisHost= new util.LinkedList[JedisPool]()
		while(!flag){
			val i: Int = new Random().nextInt(split.length)
			val hostAndPort: String = split(i)
			val split1:Array[String] = hostAndPort.split(":")
			val jedisPool = new JedisPool(this.JedisConfig, split1(0), split1(1).toInt, MainFrameConf.systemProps.getInt("jedisTimeOut"))
			if(jedisPool != null) {
				codisHost.add(jedisPool)
				flag.==(true)
			}
		}
		codisHost
	}
}
