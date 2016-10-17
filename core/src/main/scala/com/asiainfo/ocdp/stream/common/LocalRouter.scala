package com.asiainfo.ocdp.stream.common

import java.util

/**
  *
  *
  * <pre>
  * User: liuyu
  * Date: 2016/10/8
  * Time: 16:49
  * </pre>
  *
  * @author liuyu
  */
class LocalRouter extends Router {

	def this(cacheManager: String) {
		this()
		this.cacheManager = cacheManager
		val cacheManagers: Array[String] = this.cacheManager.split(",")
		for (manager <- cacheManagers) {
			//val split: Array[String] = manager.split(":")
			//this.hostMap.put(split(0), manager)
			this.hostMap.put(manager, manager)
		}
	}

	def proxyHost(host: String): String = {
		this.hostMap.filterKeys(f => {
			f.startsWith(host.trim+":")
		}).keySet.mkString(",")
	}
}
