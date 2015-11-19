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
      // deleted by surq at 2015.11.16 start
//      println("cacheServers = " + MainFrameConf.systemProps.get("cacheServers"))
//      val localip = InetAddress.getLocalHost.getHostAddress
//      val proxymap = proxylist.map(args => (args.split(":")(0), args.split(":")(1))).toMap
//      var rhost: String = null
//      var rip: String = null
//
//      proxymap.get(localip) match {
//        case Some(value) => rhost = localip
//          rip = value
//        case None =>
//          val proxyid = localip.split("\\.")(3).toInt % proxymap.size
//          rhost = proxylist(proxyid).split(":")(0)
//          rip = proxylist(proxyid).split(":")(1)
//      }
      // deleted by surq at 2015.11.16 end     
      // added by surq at 2015.11.16 start     
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
