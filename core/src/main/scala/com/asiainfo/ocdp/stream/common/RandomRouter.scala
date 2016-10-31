package com.asiainfo.ocdp.stream.common

import java.util
import java.util.Random

import com.asiainfo.ocdp.stream.config.MainFrameConf
import redis.clients.jedis.JedisPool
import redis.clients.jedis.exceptions.JedisConnectionException

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

		val linkedList = new util.ArrayList[String]()
		val toList: List[String] = this.hostMap.keySet.toList

		for(li <- toList){
			linkedList.addAll(this.hostMap.get(li).get)
		}

		var enabled = true
		var flag: Boolean = false
		while(linkedList.size() > 0 && !flag){

			//然后从机器的多个代理中挑选代理
			val i: Int = new Random().nextInt(linkedList.size())
			//println(linkedList.size())
			val hosts: String = linkedList.get(i)

			val split1:Array[String] = hosts.split(":")
			val jedis = new JedisPool(this.JedisConfig, split1(0), split1(1).toInt, MainFrameConf.systemProps.getInt("jedisTimeOut"))

			//val jedis =new JedisPool(JedisConfig,split1(0), split1(1).toInt, 3000)

			try{
				jedis.getResource
			}catch {
				case ex:JedisConnectionException =>{
					enabled = false
					linkedList.remove(i)   //如果代理不可用时，就将他从候选队列中删除
					println("本次删除的codis代理是=> hostName:【"+split1(0)+"】，port:【"+split1(1)+"】。。。")
					log.error("连接不可用。。。" + ex.getMessage)
				}
			}

			if(enabled) {
				println("本次所选的codis代理是=> hostName:【"+split1(0)+"】，port:【"+split1(1)+"】。。。")
				jedisPool = jedis
				flag = true
			}else enabled = true
		}
		jedisPool
	}
}
