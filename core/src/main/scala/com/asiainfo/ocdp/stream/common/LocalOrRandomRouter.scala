package com.asiainfo.ocdp.stream.common

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
			}*/
		}
		jedisPool
	}
}

