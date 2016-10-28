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
		}
		jedisPool
	}
}
