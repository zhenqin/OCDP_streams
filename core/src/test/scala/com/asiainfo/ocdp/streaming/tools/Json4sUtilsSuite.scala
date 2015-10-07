/*
package com.asiainfo.ocdp.streaming.tools

import com.asiainfo.ocdp.stream.tools.Json4sUtils
import org.scalatest.{BeforeAndAfter, FunSuite}


/**
 * Created by tsingfu on 15/8/18.
 */
class Json4sUtilsSuite extends FunSuite with BeforeAndAfter {

  test("1 测试 map2JsonStr"){
    val dataInterfacePropMap = collection.mutable.Map("topic" -> "topic1", "brokers" -> "hadoop1:9092,hadoop2:9092,hadoop3:9092",
      "zookeeper" -> "hadoop1:2181,hadoop2:2181,hadoop3:2181",
      "group" -> "cg1", "numThreads" -> "2")
    val jsonString = Json4sUtils.map2JsonStr(dataInterfacePropMap)

    assert(jsonString == """{"brokers":"hadoop1:9092,hadoop2:9092,hadoop3:9092","zookeeper":"hadoop1:2181,hadoop2:2181,hadoop3:2181","topic":"topic1","group":"cg1","numThreads":"2"}""")
  }

  test("2 测试 jsonStr2Map"){

    val dataInterfacePropJsonStr = """{"brokers":"hadoop1:9092,hadoop2:9092,hadoop3:9092","zookeeper":"hadoop1:2181,hadoop2:2181,hadoop3:2181","topic":"topic1","group":"cg1","numThreads":"2"}"""
    val jsonMap = Json4sUtils.jsonStr2Map(dataInterfacePropJsonStr)

    val dataInterfacePropMap = Map("topic" -> "topic1", "brokers" -> "hadoop1:9092,hadoop2:9092,hadoop3:9092",
      "zookeeper" -> "hadoop1:2181,hadoop2:2181,hadoop3:2181",
      "group" -> "cg1", "numThreads" -> "2")
    assert(jsonMap.toString == dataInterfacePropMap.toString)
  }


}
*/
