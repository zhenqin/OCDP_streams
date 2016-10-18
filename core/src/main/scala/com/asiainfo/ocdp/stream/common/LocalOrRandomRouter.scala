package com.asiainfo.ocdp.stream.common

import java.util.Random

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


	override def proxyHost(host: String): String = {
		var codisHost: String = super.proxyHost(host)
		if (codisHost == null) {
			val split: Array[String] = cacheManager.split(",")
			val i: Int = new Random().nextInt(split.length)
			codisHost = split(i)
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