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

object mains extends  App{
	private val router =  new LocalOrBanlanceRouter("s1:6379,s2:6379,s3:6379,s4:6379,s5:6379")
	for (a <- 1 to 1000) {
		println("liuyu" + router.proxyHost("s1"))
	}
}