package com.asiainfo.ocdp.stream.common

import java.net.InetAddress

import com.asiainfo.ocdp.stream.config.MainFrameConf
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
  * liuyu 修改 选择codis代理时优选选择本机，而后随机
  */
class SmartCodisCacheManager extends RedisCacheManager {

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

	    val inetAddress: InetAddress = GetHostIpOrHostName.getInetAddress()
	    val hostIp: String = GetHostIpOrHostName.getHostIp(inetAddress)
	    val hostName: String = GetHostIpOrHostName.getHostName(inetAddress)

        val proxylist = MainFrameConf.systemProps.get("cacheServers")

	    //val proxylist = "Nowledgedata1:6379,Nowledgedata1:6372,Nowledgedata2:6373"

	    val localOrRandomRouter: LocalOrRandomRouter = new LocalOrRandomRouter(proxylist)
        val hostAndPort  = if(localOrRandomRouter.proxyHost(hostName)== null || "".equals(localOrRandomRouter.proxyHost(hostName))){
	        localOrRandomRouter.proxyHost(hostName)
        }else{
	        localOrRandomRouter.proxyHost(hostIp)
        }

	    val splitInfo: Array[String] = hostAndPort.split(":")
	    val hostIpInfo: String = splitInfo(0)
	    val hostPortInfo: String = splitInfo(1)

	    (hostIpInfo,hostPortInfo)

    }
   println("get jedis pool : ip -> " + hp._1 + " ; port -> " + hp._2 +", jedisTimeOut = " + MainFrameConf.systemProps.getInt("jedisTimeOut"))
    new JedisPool(JedisConfig, hp._1, hp._2.toInt, MainFrameConf.systemProps.getInt("jedisTimeOut"))
  }

  override def getResource = jedisPool.getResource

}

