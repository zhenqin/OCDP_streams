package com.asiainfo.ocdp.stream.common

import java.util.Random

/**
  *
  *
  * <pre>
  * User: liuyu
  * Date: 2016/10/8
  * Time: 16:46
  * </pre>
  *
  * @author liuyu
  */
class RandomRouter extends Router {

	def this(cacheManager: String) {
		this()
		this.cacheManager = cacheManager
	}

	def proxyHost(host: String): String = {
		val split: Array[String] = this.cacheManager.split(",")
		val i: Int = new Random().nextInt(split.length)
		return split(i)
	}
}
