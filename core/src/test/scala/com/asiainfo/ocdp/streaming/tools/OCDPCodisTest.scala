package com.asiainfo.ocdp.streaming.tools

import java.util.List

import com.asiainfo.ocdp.stream.datasource.DataInterfaceTask
import com.asiainfo.ocdp.stream.service.{TaskServer, DataInterfaceServer}
import com.asiainfo.ocdp.stream.tools.{CacheQryThreadPool, CacheFactory}
import org.apache.commons.lang.builder.ToStringBuilder
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

import scala.collection.immutable
import scala.collection.mutable.ArrayBuffer

import java.util.concurrent.Callable
import java.util.{ List => JList, Map => JMap }
import scala.collection.convert.wrapAsScala._
import scala.collection.mutable.Map
import scala.collection.immutable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import scala.collection.mutable.ArrayBuffer

/**
  *
  *
  *
  *
  * <pre>
  *
  * Created by zhenqin.
  * User: zhenqin
  * Date: 16/10/20
  * Time: 11:38
  * Vendor: NowledgeData
  * To change this template use File | Settings | File Templates.
  *
  * </pre>
  *
  * @author zhenqin
  */
class OCDPCodisTest {


  val eventId = "test";

  val value = Array(("aaa", "1476934995454"), ("bbb", "1476934995454"))

  val JedisConfig = new JedisPoolConfig()
  JedisConfig.setMaxTotal(10)
  JedisConfig.setMaxIdle(8)
  JedisConfig.setMinIdle(2)
  JedisConfig.setMinEvictableIdleTimeMillis(100000L)
  JedisConfig.setTestOnBorrow(true)
  val jedisPool = new JedisPool(JedisConfig, "localhost", 6379, 60000)


   def query() = {
    val conn = jedisPool.getResource

    // 营销业务ID
    val event_id = eventId
    // 装载本批次所有codis key
    val rowKeyList = ArrayBuffer[String]()
    // 装载本批次数据json格式
    val jsonList = ArrayBuffer[String]()
    var resultZip: immutable.Map[String, (String, String)] = null
    try {
      val pipline = conn.pipelined()
      value.foreach(elem => {
        val rowKey = elem._1
        rowKeyList += rowKey
        jsonList += elem._2
        pipline.hmget(rowKey.getBytes("UTF-8"), event_id.getBytes("UTF-8"))
      })
      // syncAndReturnAll:hmget结果值是用list存储的，把所有items结果存储为list
      val resultJava = pipline.syncAndReturnAll
      println(resultJava)
      if (resultJava == null || resultJava.size == 0){
        resultZip
      } else {
        val result = resultJava.map(e => {
          val items = e.asInstanceOf[JList[Array[Byte]]]
          if(items(0) == null) "0" else new String(items(0))
        })
        resultZip = (rowKeyList.zip(jsonList.zip(result))).toMap
      }

    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    } finally {
      conn.close()
    }
    resultZip
  }




  def insert() = {
    val t1 = System.currentTimeMillis()
    val conn = jedisPool.getResource
    try {
      val pgl = conn.pipelined()
      //value.foreach(elem => pgl.hset(elem._1.getBytes, elem._2.getBytes, elem._3.getBytes))
      pgl.syncAndReturnAll()
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    } finally {
      conn.close()
    }
  }
}

object Boot {

  def main(args: Array[String]) {
    var inter = new DataInterfaceServer
    var events = inter.getEventsByIFId("8a934e545083152b0150843b79660007")
    events.foreach(event => {
      print(event.conf.getId + "    " + event.conf.getInterval + "    ")
      val ids = event.conf.getOutIFIds
      ids.foreach(c => {
        print(c.getInterval)
      })
      println()
    })

    println("=====================================")

    val task = new DataInterfaceTask("8a934e545083152b0150843b79660007", 30)
    task.events.foreach(event => {
      println("eventConf: " + ToStringBuilder.reflectionToString(event))
      println()
    })

    println("******************************************")


    //val taskServer = new TaskServer
    //task.makeEvents(null, taskServer.getTaskInfoById("8a934e545083152b0150849afd670008").getTid);

  }
}
