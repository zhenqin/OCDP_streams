package com.asiainfo.ocdp.stream.common

import java.util
import java.util.{Collections, Comparator}
import java.util.concurrent.atomic.AtomicInteger


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
class BanlanceRouter extends Router{
	var list = new util.LinkedList[HostAndCouter]

	def this(cacheManager: String){
		this()
		this.cacheManager=cacheManager
		val cacheManagers: Array[String] = this.cacheManager.split(",")
		for (manager <- cacheManagers) {
			this.list.add(new HostAndCouter(manager))
		}
	}

	override def proxyHost(host: String): String = {
		Collections.sort(this.list, new Comparator[HostAndCouter]() {
			def compare(o1: HostAndCouter, o2: HostAndCouter): Int = {
				 o1.get - o2.get
			}
		})
		val first: HostAndCouter = this.list.getFirst
		first.incrementAndGet
		first.host
	}
}


 class HostAndCouter {
	 var host: String = null
	 var counter: AtomicInteger = new AtomicInteger(0)

	def this(host: String) {
		this()
		this.host = host
	}

	def incrementAndGet: Int = {
		 counter.incrementAndGet
	}

	def decrementAndGet: Int = {
		 counter.decrementAndGet
	}

	def intValue: Int = {
		 counter.intValue
	}

	def get: Int = {
		 counter.get
	}

	def setHost(host: String) {
		this.host = host
	}

	def setCounter(counter: AtomicInteger) {
		this.counter = counter
	}


	override def equals(obj: scala.Any): Boolean = {
		if (this == obj) return true
		if (!(obj.isInstanceOf[HostAndCouter])) return false
		val that:HostAndCouter = obj.asInstanceOf[HostAndCouter]

		host == that.host
	}

	override def hashCode: Int = {
		host.hashCode
	}
}

object main extends  App{
	private val router: BanlanceRouter = new BanlanceRouter("s1:6379,s2:6379,s3:6379,s4:6379,s5:6379")
	for (a <- 1 to 1000) {
		println(router.proxyHost("s9"))
	}
}