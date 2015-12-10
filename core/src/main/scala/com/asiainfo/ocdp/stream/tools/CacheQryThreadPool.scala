package com.asiainfo.ocdp.stream.tools

import java.util.concurrent.Callable
import java.util.{ List => JList, Map => JMap }
import com.asiainfo.ocdp.stream.common.CodisCacheManager
import com.asiainfo.ocdp.stream.config.MainFrameConf
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
import scala.collection.convert.wrapAsScala._
import scala.collection.mutable.Map
import scala.collection.mutable
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
      //      println("Insert " + value.size + " key cost " + (System.currentTimeMillis() - t1) + " Millis")
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
 * 保存 Array[(Row_rowKey,(eventId, Row)] => Map[Row_rowKey, Map(eventId, Row)]
 * @param value
 */
//class InsertEventRows(value: Array[(String, String, String)]) extends Callable[String] {
//  val logger = LoggerFactory.getLogger(this.getClass)
//
//  override def call() = {
//    val conn = CacheFactory.getManager.asInstanceOf[CodisCacheManager].getResource
//
//    try {
//      val pgl = conn.pipelined()
//      val ite = value.iterator
//      while (ite.hasNext) {
//        val elem = ite.next()
//        val rowKey = elem._1
//        val fieldEventId = elem._2
//        val jsonRow = elem._3
//        pgl.hset(rowKey.getBytes, fieldEventId.getBytes, jsonRow.getBytes)
//      }
//      pgl.syncAndReturnAll()
//
//    } catch {
//      case ex: Exception =>
//        logger.error("= = " * 15 + "found error in InsertEventRows.call()")
//    } finally {
//      conn.close()
//    }
//
//    ""
//  }
//}

/**
 * 存储各业务的结果（等待event复用）
 * value: hset: ( eventCache:unikey1:unikey2,Row:eventId:eventID,json)
 * key: eventCache:unikey1:unikey2 item: Row:eventId:eventID value: json
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
//      println("InsertEventRows " + value.size + " key cost " + (System.currentTimeMillis() - t1) + " Millis")
    }
  }
}

/**
 * 获取事件缓存
 * Array[(Row_rowKey, Array(eventId/businessEventId)]
 * @param value `Map[Row_rowKey, Map[(eventId/businessEventId, Row/time)]]`
 *
 */
//class QryEventCache(value: Array[(String, Array[String])]) extends Callable[Map[String, Map[String, String]]] {
//  val logger = LoggerFactory.getLogger(this.getClass)
//
//  override def call() = {
//    val conn = CacheFactory.getManager.asInstanceOf[CodisCacheManager].getResource
//
//    val resultMap = Map[String, Map[String, String]]()
//
//    try {
//      val pgl = conn.pipelined()
//      val ite = value.iterator
//      while (ite.hasNext) {
//        var result: JList[Array[Byte]] = null
//        val elem = ite.next() //结构：(Row_rowKey,Array(eventId))
//        val rowKey = elem._1
//        val fields = elem._2
//        pgl.hmget(rowKey.getBytes(CacheQryThreadPool.DEFAULT_CHARACTER_SET), fields.map(_.getBytes(CacheQryThreadPool.DEFAULT_CHARACTER_SET)): _*)
//        result = pgl.syncAndReturnAll().head.asInstanceOf[JList[Array[Byte]]]
//
//        if (!resultMap.contains(rowKey)) {
//          resultMap.put(rowKey, Map[String, String]())
//        }
//        fields.zip(result).foreach {
//          case (k, v) =>
//            if (v != null) resultMap.get(rowKey).get.put(k, new String(v))
//        }
//      }
//
//    } catch {
//      case ex: Exception =>
//        logger.error("= = " * 15 + "found error in QryEventCache.call()")
//        ex.printStackTrace()
//    } finally {
//      conn.close()
//    }
//
//    resultMap
//  }
//}

/**
 * value:(eventCache:eventKeyValue,jsonValue)
 * result: Map[rowKeyList->Tuple2(jsonList->result)]
 */
class QryEventCache(value: Array[(String, String)], eventId: String) extends Callable[immutable.Map[String, (String, Array[Byte])]] {
  val logger = LoggerFactory.getLogger(this.getClass)

  override def call() = {
    val conn = CacheFactory.getManager.asInstanceOf[CodisCacheManager].getResource

    // 营销业务ID
    val fields = eventId
    // 装载本批次所有codis key
    val rowKeyList = ArrayBuffer[String]()
    // 装载本批次数据json格式
    val jsonList = ArrayBuffer[String]()
    var resultZip: immutable.Map[String, (String, Array[Byte])] = null
    try {
      val pgl = conn.pipelined()
      value.foreach(elem => {
        val rowKey = elem._1
        rowKeyList += rowKey
        jsonList += elem._2
        pgl.hmget(rowKey.getBytes(CacheQryThreadPool.DEFAULT_CHARACTER_SET), fields.getBytes(CacheQryThreadPool.DEFAULT_CHARACTER_SET))
      })
      val result = pgl.syncAndReturnAll().head.asInstanceOf[JList[Array[Byte]]]
      resultZip = (rowKeyList.zip(jsonList.zip(result))).toMap
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

/**
 * 获取事件缓存
 * Array[(Row_rowKey, Array(eventId/businessEventId)]
 * @param value `Map[Row_rowKey, Map[(eventId/businessEventId, Row/time)]]`
 *
 */
//class QryAllEventCache(value: mutable.Set[String]) extends Callable[Map[String, Map[String, Array[Byte]]]] {
//  val logger = LoggerFactory.getLogger(this.getClass)
//
//  override def call() = {
//    val conn = CacheFactory.getManager.asInstanceOf[CodisCacheManager].getResource
//
//    val resultMap = Map[String, Map[String, Array[Byte]]]()
//
//    try {
//      val tool = new KryoSerializerStreamAppTool
//
//      val pgl = conn.pipelined()
//      val ite = value.iterator
//      while (ite.hasNext) {
//        var result: JMap[Array[Byte], Array[Byte]] = null
//        val rowKey = ite.next() //结构：(Row_rowKey,Array(eventId))
//        pgl.hgetAll(rowKey.getBytes(CacheQryThreadPool.DEFAULT_CHARACTER_SET))
//        result = pgl.syncAndReturnAll().head.asInstanceOf[JMap[Array[Byte], Array[Byte]]]
//
//        if (!resultMap.contains(rowKey)) {
//          resultMap.put(rowKey, Map[String, Array[Byte]]())
//        }
//        result.foreach {
//          case (k, v) =>
//            resultMap.get(rowKey).get.put(new String(k, CacheQryThreadPool.DEFAULT_CHARACTER_SET), v)
//        }
//      }
//
//    } catch {
//      case ex: Exception =>
//        logger.error("= = " * 15 + "found error in QryAllEventCache.call()")
//        ex.printStackTrace()
//    } finally {
//      conn.close()
//    }
//
//    resultMap
//  }
//}