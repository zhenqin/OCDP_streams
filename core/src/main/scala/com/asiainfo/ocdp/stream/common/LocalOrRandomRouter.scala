package com.asiainfo.ocdp.stream.common

<<<<<<< HEAD
import java.util
import java.util.Random
=======
import redis.clients.jedis.JedisPool

>>>>>>> a0970eb026525ff329149c04c794a378b9fb70d0

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

<<<<<<< HEAD
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
=======
	override def proxyHost(host: String): JedisPool = {
		var jedisPool: JedisPool = null
		jedisPool = super.proxyHost(host)

		if (jedisPool == null) {

			jedisPool = new RandomRouter(cacheManager).proxyHost(host)

			/*val linkedList = new util.ArrayList[String]()
			val toList: List[String] = this.hostMap.keySet.toList

			for(li <- toList){
				linkedList.addAll(this.hostMap.get(li).get)
			}

			var enabled = true
			var flag: Boolean = false

			while(linkedList.size()!= 0 && !flag){
				//随机从多个代理中挑选代理
				val i: Int = new Random().nextInt(linkedList.size())
				val host: String = linkedList.get(i)

				val split1:Array[String] = host.split(":")
				//val jedis = new JedisPool(this.JedisConfig, split1(0), split1(1).toInt, MainFrameConf.systemProps.getInt("jedisTimeOut"))

				val jedis =new JedisPool(JedisConfig,split1(0), split1(1).toInt, 3000)

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
=======
					jedisPool = jedis
					flag = true
				}
			}*/
		}
		jedisPool
	}
}

>>>>>>> a0970eb026525ff329149c04c794a378b9fb70d0
