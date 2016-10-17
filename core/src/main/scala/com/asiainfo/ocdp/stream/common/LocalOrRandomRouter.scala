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
		if (codisHost == null || "".equals(codisHost)) {
			val split: Array[String] = cacheManager.split(",")
			val i: Int = new Random().nextInt(split.length)
			codisHost = split(i)
		}
		codisHost
	}
}
