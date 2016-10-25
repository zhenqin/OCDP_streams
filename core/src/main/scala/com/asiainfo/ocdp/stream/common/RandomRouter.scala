package com.asiainfo.ocdp.stream.common

import java.util
import java.util.Random

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
  * Date: 2016/10/8
  * Time: 16:46
  * </pre>
  *
  * @author liuyu
  */
class RandomRouter(cacheManager: String) extends Router(cacheManager) {

	def proxyHost(host: String): JedisPool = {
		var jedisPool: JedisPool = null

		val linkedList = new util.ArrayList[String]()
		val toList: List[String] = this.hostMap.keySet.toList

		for(li <- toList){
			linkedList.addAll(this.hostMap.get(li).get)
		}

		var enabled = true
		var flag: Boolean = false
		while(linkedList.size()!= 0 && !flag){

			//然后从机器的多个代理中挑选代理
			val i: Int = new Random().nextInt(linkedList.size())
			val host: String = linkedList.get(i)

			val split1:Array[String] = host.split(":")
			//val jedis = new JedisPool(this.JedisConfig, split1(0), split1(1).toInt, MainFrameConf.systemProps.getInt("jedisTimeOut"))

			val jedis =new JedisPool(JedisConfig,split1(0), split1(1).toInt, 3000)

			try{
				jedisPool.getResource
			}catch {
				case _ =>{
					enabled = false
					log.error("连接不可用。。。")
				}
			}

<<<<<<< HEAD
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
=======
			if(enabled) {
				jedisPool = jedis
				flag = true
			}
		}
		jedisPool
>>>>>>> a0970eb026525ff329149c04c794a378b9fb70d0
	}
}
