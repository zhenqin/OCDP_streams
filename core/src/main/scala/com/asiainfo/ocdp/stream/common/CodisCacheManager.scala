package com.asiainfo.ocdp.stream.common

import java.net.InetAddress
import com.asiainfo.ocdp.stream.config.MainFrameConf
import redis.clients.jedis.{JedisPoolConfig, JedisPool}

/**
 * Created by leo on 8/12/15.
 */
class CodisCacheManager extends RedisCacheManager {

  private val jedisPool: JedisPool = {

    val JedisConfig = new JedisPoolConfig()
	  JedisConfig.setMaxTotal(MainFrameConf.systemProps.getInt("jedisPoolMaxTotal"))
	  JedisConfig.setMaxIdle(MainFrameConf.systemProps.getInt("jedisPoolMaxIdle"))
	  JedisConfig.setMinIdle(MainFrameConf.systemProps.getInt("jedisPoolMinIdle"))
	  JedisConfig.setMinEvictableIdleTimeMillis(MainFrameConf.systemProps.getInt("jedisPoolMEM"))

	  println("jedisPoolMaxTotal = " + MainFrameConf.systemProps.getInt("jedisPoolMaxTotal") +
		  ", jedisPoolMaxIdle = " + MainFrameConf.systemProps.getInt("jedisPoolMaxIdle") +
		  ", jedisPoolMinIdle = " + MainFrameConf.systemProps.getInt("jedisPoolMinIdle") +
		  ", jedisPoolMEM = " + MainFrameConf.systemProps.getInt("jedisPoolMEM")
	  )
    JedisConfig.setTestOnBorrow(true)

    val hp: Tuple2[String, String] = {
      val proxylist = MainFrameConf.systemProps.get("cacheServers").split(",")
	    //TODO 随机选择代理是否会有瓶颈？是否可以设置均匀的选择代理
       val proxyid  = new java.util.Random().nextInt(proxylist.size)
       val proxyInfo =proxylist(proxyid).split(":")
       val rhost = proxyInfo(0)
       val rip = proxyInfo(1)
       // added by surq at 2015.11.16 end     
      (rhost, rip)
    }
    println("get jedis pool : ip -> " + hp._1 + " ; port -> " + hp._2 +", jedisTimeOut = " + MainFrameConf.systemProps.getInt("jedisTimeOut"))
    new JedisPool(JedisConfig, hp._1, hp._2.toInt, MainFrameConf.systemProps.getInt("jedisTimeOut"))
  }

  override def getResource = jedisPool.getResource

}
