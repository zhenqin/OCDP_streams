package com.asiainfo.ocdp.stream.common

import java.net.InetAddress
import java.util
import java.util.Random

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
	 val hostAndPort: util.LinkedList[JedisPool] = localOrRandomRouter.proxyHost(hostName)

	 var proxyHostAndPort:JedisPool = null
	 val size: Int = hostAndPort.size()
	 if (hostAndPort != null && size!= 0) {
		 val i: Int = new Random().nextInt(size)
		 proxyHostAndPort=hostAndPort.get(i)
	 }

	 proxyHostAndPort
  }

  override def getResource = jedisPool.getResource

}

