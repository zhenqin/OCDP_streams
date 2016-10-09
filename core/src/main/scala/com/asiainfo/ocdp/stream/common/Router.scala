package com.asiainfo.ocdp.stream.common

import java.util

/**
  *
  *
  * <pre>
  * User: liuyu
  * Date: 2016/10/8
  * Time: 16:45
  * </pre>
  *
  * @author liuyu
  */
abstract class Router {

	var cacheManager: String=""
	val hostMap= new util.HashMap[String, String]

	def this(cacheManager: String) {
		this()
		this.cacheManager=cacheManager
	}

	def proxyHost(host: String): String
}
