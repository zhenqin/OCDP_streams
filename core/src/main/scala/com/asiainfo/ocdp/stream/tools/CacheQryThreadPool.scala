package com.asiainfo.ocdp.stream.tools

import java.util.concurrent.Callable
import java.util.{ List => JList, Map => JMap }
import com.asiainfo.ocdp.stream.common.CodisCacheManager
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
import scala.collection.convert.wrapAsScala._
import scala.collection.mutable.Map
import scala.collection.immutable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import scala.collection.mutable.ArrayBuffer

/**
 * Created by by surq on 12/09/15
 */
object CacheQryThreadPool {
  // 初始化线程池
  val threadPool: ExecutorService = Executors.newCachedThreadPool
  //  val threadPool = ThreadUtils.newDaemonCachedThreadPool("CacheQryDaemonCachedThreadPool", MainFrameConf.systemProps.getInt("cacheQryThreadPoolSize"))

  val DEFAULT_CHARACTER_SET = "UTF-8"
}

class Qry(keyList: List[String]) extends Callable[List[(String, Array[Byte])]] {
  val logger = LoggerFactory.getLogger(this.getClass)
  override def call() = {
    val t1 = System.currentTimeMillis()
    val keys = keyList.map(x => x.getBytes).toSeq
    val conn = CacheFactory.getManager.asInstanceOf[CodisCacheManager].getResource
    var result: JList[Array[Byte]] = null
    try {
      val pipeline = conn.pipelined()
      keys.foreach(key => pipeline.get(key))
      val pipline_result = pipeline.syncAndReturnAll()
      result = pipline_result.asInstanceOf[JList[Array[Byte]]]
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        logger.error("= = " * 15 + "found error in Qry.call()" + "surq:" + ex.getStackTraceString)
        throw ex
    } finally {
      conn.close()
    }
    if (result != null) keyList.zip(result) else null
  }
}

class QryHashall(keys: Seq[String]) extends Callable[Seq[(String, java.util.Map[String, String])]] {
  val logger = LoggerFactory.getLogger(this.getClass)

  override def call() = {
    val t1 = System.currentTimeMillis()
    val conn = CacheFactory.getManager.asInstanceOf[CodisCacheManager].getResource
    var result: JList[JMap[String, String]] = null
    try {
      val pgl = conn.pipelined()
      keys.foreach(x => pgl.hgetAll(x))
      val result_tmp = pgl.syncAndReturnAll()
      result = result_tmp.asInstanceOf[JList[JMap[String, String]]]
    } catch {
      case ex: Exception =>
        logger.error("= = " * 15 + "found error in QryHashall.call()")
    } finally {
      conn.close()
    }
    if (result != null) keys.zip(result) else null
  }
}

class Insert(value: Map[String, Any]) extends Callable[String] {
  val logger = LoggerFactory.getLogger(this.getClass)

  override def call() = {
    val t1 = System.currentTimeMillis()
    val conn = CacheFactory.getManager.asInstanceOf[CodisCacheManager].getResource
    try {
      val pipeline = conn.pipelined()
      val ite = value.iterator
      val kryotool = new KryoSerializerStreamAppTool
      //      value.foreach(elem => pipeline.set(elem._1.getBytes, kryotool.serialize(elem._2).array()))
      while (ite.hasNext) {
        val elem = ite.next()
        pipeline.set(elem._1.getBytes, kryotool.serialize(elem._2).array())
        //            pipeline.sync()
      }
      pipeline.sync()
      println("Insert " + value.size + " key cost " + (System.currentTimeMillis() - t1) + " Millis")
    } catch {
      case ex: Exception =>
        logger.error("= = " * 15 + "found error in Insert.call()")
    } finally {
      conn.close()
    }
    ""
  }
}

class InsertHash(value: Map[String, Map[String, String]]) extends Callable[String] {
  val logger = LoggerFactory.getLogger(this.getClass)

  override def call() = {
    val t1 = System.currentTimeMillis()
    val conn = CacheFactory.getManager.asInstanceOf[CodisCacheManager].getResource

    try {
      val pgl = conn.pipelined()
      val ite = value.iterator
      while (ite.hasNext) {
        val elem = ite.next()
        pgl.hmset(elem._1, elem._2.asJava)
      }
      //      println("InsertHash " + value.size + " key cost " + (System.currentTimeMillis() - t1) + " Millis")
      pgl.sync()
    } catch {
      case ex: Exception =>
        logger.error("= = " * 15 + "found error in InsertHash.call()")
    } finally {
      conn.close()

    }

    ""
  }
}

/**
 * 存储各业务的结果（等待event复用）
 * value: hset: ( eventCache:unikey1:unikey2,Row:eventId:eventID,time)
 * key: eventCache:unikey1:unikey2 item: Row:eventId:eventID value: time
 */
class InsertEventRows(value: Array[(String, String, String)]) extends Runnable {
  val logger = LoggerFactory.getLogger(this.getClass)

  override def run() = {
    val t1 = System.currentTimeMillis()
    val conn = CacheFactory.getManager.asInstanceOf[CodisCacheManager].getResource
    try {
      val pgl = conn.pipelined()
      value.foreach(elem => pgl.hset(elem._1.getBytes, elem._2.getBytes, elem._3.getBytes))
      pgl.syncAndReturnAll()
    } catch {
      case ex: Exception =>
        logger.error("= = " * 15 + "found error in InsertEventRows.call()")
    } finally {
      conn.close()
    }
  }
}

/**
 * value:(eventCache:eventKeyValue,jsonValue)
 * result: Map[rowKeyList->Tuple2(jsonList->result)]
 */
class QryEventCache(value: Array[(String, String)], eventId: String) extends Callable[immutable.Map[String, (String, Array[Byte])]] {
  val logger = LoggerFactory.getLogger(this.getClass)
//  import scala.collection.JavaConverters._
  override def call() = {
    val conn = CacheFactory.getManager.asInstanceOf[CodisCacheManager].getResource

    // 营销业务ID
    val event_id = eventId
    // 装载本批次所有codis key
    val rowKeyList = ArrayBuffer[String]()
    // 装载本批次数据json格式
    val jsonList = ArrayBuffer[String]()
    var resultZip: immutable.Map[String, (String, Array[Byte])] = null
    try {
      val pipline = conn.pipelined()
      value.foreach(elem => {
        val rowKey = elem._1
        rowKeyList += rowKey
        jsonList += elem._2
        pipline.hmget(rowKey.getBytes(CacheQryThreadPool.DEFAULT_CHARACTER_SET), event_id.getBytes(CacheQryThreadPool.DEFAULT_CHARACTER_SET))
      })
      // syncAndReturnAll:hmget结果值是用list存储的，把所有items结果存储为list
      val resultJava = pipline.syncAndReturnAll
      if (resultJava == null || resultJava.size == 0) resultZip  else {
        val result = resultJava.map(e => {val items =e.asInstanceOf[JList[Array[Byte]]];items(0)})
        resultZip = (rowKeyList.zip(jsonList.zip(result))).toMap
      }

    } catch {
      case ex: Exception =>
        logger.error("= = " * 15 + "found error in QryEventCache.call()")
        ex.printStackTrace()
    } finally {
      conn.close()
    }
    resultZip
  }
}