package com.asiainfo.ocdp.stream.common

import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Collections, Comparator}

import com.asiainfo.ocdp.stream.config.MainFrameConf
import redis.clients.jedis.JedisPool


/**
  *
  *
  * <pre>
  * User: liuyu
  * Date: 2016/10/8
  * Time: 16:55
  * </pre>
  *
  * @author liuyu
  */
class BanlanceRouter extends Router{
	val list = new util.LinkedList[HostAndCouter]

	def this(cacheManager: String){
		this()
		this.cacheManager=cacheManager
		val cacheManagers: Array[String] = this.cacheManager.split(",")
		for (manager <- cacheManagers) {
			this.list.add(new HostAndCouter(manager))
		}
	}

<<<<<<< HEAD
	override def proxyHost(host: String): util.LinkedList[JedisPool] = {
=======
	override def proxyHost(host: String): JedisPool = {
>>>>>>> a0970eb026525ff329149c04c794a378b9fb70d0
		Collections.sort(this.list, new Comparator[HostAndCouter]() {
			def compare(o1: HostAndCouter, o2: HostAndCouter): Int = {
				 o1.get - o2.get
			}
		})

<<<<<<< HEAD
		var enabled = true
		var flag: Boolean = false
		var index: Int = 0
		val codisHost= new util.LinkedList[JedisPool]()
=======
		var jedisPool: JedisPool = null
		var enabled = true
		var flag: Boolean = false
		var index: Int = 0
>>>>>>> a0970eb026525ff329149c04c794a378b9fb70d0

		while(!flag){
			val first: HostAndCouter = this.list.get(index)
			val split1:Array[String] = first.host.split(":")
<<<<<<< HEAD
			val jedisPool = new JedisPool(this.JedisConfig, split1(0), split1(1).toInt, MainFrameConf.systemProps.getInt("jedisTimeOut"))

			try{
				jedisPool.getResource
=======
			val jedis = new JedisPool(this.JedisConfig, split1(0), split1(1).toInt, MainFrameConf.systemProps.getInt("jedisTimeOut"))

			try{
				jedis.getResource
>>>>>>> a0970eb026525ff329149c04c794a378b9fb70d0
			}catch {
				case _ =>{
					enabled = false
					log.error("连接不可用。。。")
				}
			}

			if(enabled) {
<<<<<<< HEAD
				codisHost.add(jedisPool)
=======
				jedisPool = jedis
>>>>>>> a0970eb026525ff329149c04c794a378b9fb70d0
				first.incrementAndGet
				index = index+1
				flag = true
			}
		}
<<<<<<< HEAD

		codisHost
=======
		jedisPool
>>>>>>> a0970eb026525ff329149c04c794a378b9fb70d0
	}
}


class HostAndCouter {
	var host: String = null
	var counter: AtomicInteger = new AtomicInteger(0)

	def this(host: String) {
		this()
		this.host = host
	}

	def incrementAndGet: Int = {
		counter.incrementAndGet
	}

	def decrementAndGet: Int = {
		counter.decrementAndGet
	}

	def intValue: Int = {
		counter.intValue
	}

	def get: Int = {
		counter.get
	}

	def setHost(host: String) {
		this.host = host
	}

	def setCounter(counter: AtomicInteger) {
		this.counter = counter
	}


	override def equals(obj: scala.Any): Boolean = {
		if (this == obj) return true
		if (!(obj.isInstanceOf[HostAndCouter])) return false
		val that:HostAndCouter = obj.asInstanceOf[HostAndCouter]

		host == that.host
	}

	override def hashCode: Int = {
		host.hashCode
	}
}