package com.asiainfo.ocdp.stream.common

import java.net.InetAddress

import com.asiainfo.ocdp.stream.config.MainFrameConf
import redis.clients.jedis.JedisPool

/**
  * liuyu 修改 选择codis代理时优选选择本机，而后随机
  */
class SmartCodisCacheManager extends RedisCacheManager {

 private val jedisPool: JedisPool = {

	 val inetAddress: InetAddress = GetHostIpOrHostName.getInetAddress()
	 val hostIp: String = GetHostIpOrHostName.getHostIp(inetAddress)  //获取当前主机Ip
	 val hostName: String = GetHostIpOrHostName.getHostName(inetAddress) //获取当前主机hostname

	 val proxylist = MainFrameConf.systemProps.get("cacheServers")

	 val localOrRandomRouter: LocalOrRandomRouter = new LocalOrRandomRouter(proxylist)
	 val jedisPool: JedisPool = localOrRandomRouter.proxyHost(hostName)

	 jedisPool
  }

  override def getResource = jedisPool.getResource

}

