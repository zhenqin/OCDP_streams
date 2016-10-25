package com.asiainfo.ocdp.stream.common

import java.util

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
		}
		jedisPool
	}
}
