package com.asiainfo.ocdp.stream.common

import com.asiainfo.ocdp.stream.config.MainFrameConf
import com.wandoulabs.jodis.{JedisResourcePool, RoundRobinJedisPool}
import redis.clients.jedis.JedisPoolConfig

/**
 * Created by leo on 8/12/15.
 */
class JodisCacheManager extends RedisCacheManager {

  override def getResource = jedisPool.getResource

  private val jedisPool:JedisResourcePool = {

    val JedisConfig = new JedisPoolConfig()
    JedisConfig.setMaxIdle(MainFrameConf.systemProps.getInt("JedisMaxIdle"))
    JedisConfig.setMaxTotal(MainFrameConf.systemProps.getInt("JedisMaxTotal"))
    JedisConfig.setMinEvictableIdleTimeMillis(MainFrameConf.systemProps.getInt("JedisMEM"))
    JedisConfig.setTestOnBorrow(true)
    new RoundRobinJedisPool(MainFrameConf.systemProps.get("zk"),
      MainFrameConf.systemProps.getInt("zkSessionTimeoutMs"),
      MainFrameConf.systemProps.get("zkpath"),
      JedisConfig)
  }
}
