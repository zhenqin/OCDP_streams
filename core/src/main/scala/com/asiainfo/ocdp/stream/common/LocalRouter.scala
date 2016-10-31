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

			//println(hostList.size())
			//val iterator: util.Iterator[String] = hostList.iterator()
			var flag: Boolean = false
			var enabled = true  //链接是否可用的标志

			while (/*iterator.hasNext*/ hostList.size()>0 && !flag){
				//val element: String = iterator.next()
				val i: Int = new Random().nextInt(hostList.size())

				val hostAndPort:Array[String] = hostList.get(i).split(":")
				val jedis= new JedisPool(this.JedisConfig, hostAndPort(0), hostAndPort(1).toInt, MainFrameConf.systemProps.getInt("jedisTimeOut"))

				//val jedis =new JedisPool(JedisConfig,hostAndPort(0), hostAndPort(1).toInt, 3000)
				//通过是否抛异常判断连接是否可用
				try{
					jedis.getResource
				}catch {
					case ex:JedisConnectionException =>{
						enabled = false
						hostList.remove(i)   //如果本机器的某个代理不可用，就将其从候选队列删除
						log.error("连接不可用。。。" + ex.getMessage)
					}
				}

				if(enabled) {
					println("本次所选的codis代理是=> hostName:【"+hostAndPort(0)+"】，port:【"+hostAndPort(1)+"】。。。")
					jedisPool = jedis
					flag = true
				}else enabled = true
			}
		}
		jedisPool
	}
}
