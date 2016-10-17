package com.asiainfo.ocdp.streaming.tools

import java.net.InetAddress

/**
  *
  *
  * <pre>
  * User: liuyu
  * Date: 2016/10/16
  * Time: 10:45
  * </pre>
  *
  * @author liuyu
  */
object LyTest {
	def reduceByKeyFun = (jsonLabelStr1:String, jsonLabelStr2:String) => {
		if(jsonLabelStr1 > jsonLabelStr2) jsonLabelStr1
		else jsonLabelStr2
	}

	def main(args:Array[String]): Unit ={
		val result=reduceByKeyFun("c","ba")
		println(result)
		val integer: Integer = null.asInstanceOf[Integer]
		println(null.asInstanceOf[Integer])
		val address: String = InetAddress.getLocalHost.getHostAddress
		println(address)


		val map = Map(("test1" -> "test1"),("test2" -> "test2"))

		map.filterKeys(f => {
			f.startsWith("test")
		}).toList
	}
}
