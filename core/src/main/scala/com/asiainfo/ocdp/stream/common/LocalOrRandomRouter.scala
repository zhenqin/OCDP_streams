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
  * Time: 16:55
  * </pre>
  *
  * @author liuyu
  */
class LocalOrRandomRouter(cacheManager: String) extends LocalRouter(cacheManager) {

	override def proxyHost(host: String): util.LinkedList[JedisPool] = {
		var codisHost= super.proxyHost(host)

		var enabled = true
		var flag: Boolean = false
		if (codisHost!=null && codisHost.size()==0) {
			val split: Array[String] = cacheManager.split(",")
			while(!flag){
				val i: Int = new Random().nextInt(split.length)
				val hostAndPort: String = split(i)
				val split1:Array[String] = hostAndPort.split(":")
				val jedisPool = new JedisPool(this.JedisConfig, split1(0), split1(1).toInt, MainFrameConf.systemProps.getInt("jedisTimeOut"))

				//val jedisPool =new JedisPool(JedisConfig,split1(0), split1(1).toInt, 3000)

				try{
					jedisPool.getResource
				}catch {
					case _ =>{
						enabled = false
						log.error("连接不可用。。。")
					}
				}

				if(enabled) {
					codisHost.add(jedisPool)
					flag = true
				}
			}
		}

		codisHost
	}
}

object LocalOrRandomRouter extends  App{
	private val router: LocalOrRandomRouter = new LocalOrRandomRouter("s1:6379,s2:6379,s3:6379,s4:6379,s5:6379")
	for (a <- 1 to 1000) {
		println(router.proxyHost("s9"))
	}
}
