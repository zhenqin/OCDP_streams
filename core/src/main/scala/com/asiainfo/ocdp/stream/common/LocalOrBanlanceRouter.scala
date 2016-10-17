package com.asiainfo.ocdp.stream.common

/**
  *
  *
  * <pre>
  * User: liuyu
  * Date: 2016/10/9
  * Time: 10:31
  * </pre>
  *
  * @author liuyu
  */
class LocalOrBanlanceRouter  extends BanlanceRouter{

	def this(cacheManager: String) {
		this()
		this.cacheManager=cacheManager
		val cacheManagers: Array[String] = this.cacheManager.split(",")
		for (manager <- cacheManagers) {
			val split: Array[String] = manager.split(":")
			this.hostMap.put(split(0), manager)
			this.list.add(new HostAndCouter(manager))
		}
	}

	override def proxyHost(host: String): String = {
		var codisHost: String = this.hostMap.get(host)
		if (codisHost == null) {
			codisHost = super.proxyHost(host)
		}
		codisHost
	}
}