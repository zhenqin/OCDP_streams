package com.asiainfo.ocdp.stream.common

import java.nio.ByteBuffer
import java.util.concurrent.FutureTask
import java.util.{ ArrayList => JArrayList, List => JList, Map => JMap }
import com.asiainfo.ocdp.stream.config.MainFrameConf
import com.asiainfo.ocdp.stream.constant.EventConstant
import com.asiainfo.ocdp.stream.tools._
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis
import scala.collection.convert.wrapAsJava.mapAsJavaMap
import scala.collection.convert.wrapAsScala._
import scala.collection.mutable.Map
import java.util.concurrent.ExecutionException

/**
 * Created by surq on 12/09/15
 */
abstract class RedisCacheManager extends CacheManager {

  val logger = LoggerFactory.getLogger(this.getClass)

  private val currentJedis = new ThreadLocal[Jedis] {
    override def initialValue = getResource
  }

  private val currentKryoTool = new ThreadLocal[KryoSerializerStreamAppTool] {
    override def initialValue = new KryoSerializerStreamAppTool
  }
  val miniBatch = MainFrameConf.systemProps.getInt("cacheQryTaskSizeLimit")
  //  private val currentJedis = getResource
  final def openConnection = currentJedis.set(getResource)

  final def getConnection = {
    val curr_jedis = currentJedis.get()
    curr_jedis
  }

  final def getKryoTool = currentKryoTool.get()

  //final def getConnection = currentJedis

  final def closeConnection = {
    getConnection.close()
    currentJedis.remove()
  }

  def getResource: Jedis

  override def getHashCacheList(key: String): List[String] = {
    getConnection.lrange(key, 0, -1).toList
  }

  override def getHashCacheMap(key: String): Map[String, String] = {
    val t1 = System.currentTimeMillis()
    val value = getConnection.hgetAll(key)
    System.out.println("GET 1 key from userinfo cost " + (System.currentTimeMillis() - t1))
    value
  }

  override def setHashCacheString(key: String, value: String): Unit = {
    getConnection.set(key, value)
  }

  override def getCommonCacheValue(cacheName: String, key: String): String = {
    val t1 = System.currentTimeMillis()
    val value = getConnection.hget(cacheName, key)
    System.out.println("GET 1 key from lacci cost " + (System.currentTimeMillis() - t1))
    value
  }

  override def getHashCacheString(key: String): String = {
    getConnection.get(key)
  }

  override def getCommonCacheMap(key: String): Map[String, String] = {
    getConnection.hgetAll(key)
  }

  override def getCommonCacheList(key: String): List[String] = {
    getConnection.lrange(key, 0, -1).toList
  }

  override def setHashCacheMap(key: String, value: Map[String, String]): Unit = {
    val t1 = System.currentTimeMillis()
    getConnection.hmset(key, mapAsJavaMap(value))
    System.out.println("SET 1 key cost " + (System.currentTimeMillis() - t1))
  }

  override def setHashCacheList(key: String, value: List[String]): Unit = {
    value.map { x => getConnection.rpush(key, x) }
  }

  override def setByteCacheString(key: String, value: Any) {
    val t1 = System.currentTimeMillis()
    val r = getConnection.set(key.getBytes, getKryoTool.serialize(value).array())
    System.out.println("SET 1 key cost " + (System.currentTimeMillis() - t1))
  }

  override def getByteCacheString(key: String): Any = {
    val t1 = System.currentTimeMillis()
    val bytekey = key.getBytes
    val cachedata = getConnection.get(bytekey)

    val t2 = System.currentTimeMillis()
    System.out.println("GET 1 key cost " + (t2 - t1))

    if (cachedata != null) {
      getKryoTool.deserialize[Any](ByteBuffer.wrap(cachedata))
    } else null

  }


  // added by surq at 2015.12.7 start-----------------UPDATA-------------
  override def setMultiCache(keysvalues: Map[String, Any]) {
    val kvSplit = keysvalues.sliding(miniBatch, miniBatch).toList
    kvSplit.foreach(limitBatch => CacheQryThreadPool.threadPool.submit(new FutureTask[String](new Insert(limitBatch))))
  }

  override def getMultiCacheByKeys(keys: List[String],
    qryCacheService: java.util.concurrent.ExecutorCompletionService[List[(String, Array[Byte])]]): Map[String, Any] = {
    val resultMap = Map[String, Any]()
    val start = System.currentTimeMillis();
    // 把原数据划分小批次多线程查询
    val keySplit = keys.sliding(miniBatch, miniBatch).toList
    try {
      keySplit.map(keyList => qryCacheService.submit(new Qry(keyList)))
      for (index <- 0 until keySplit.size) {
        // 把查询的结果集放入multimap
        val result = qryCacheService.take().get
        if (result != null) {
          result.foreach(rs => 
            if (rs._2 != null && rs._2.length > 0) resultMap += (rs._1 -> getKryoTool.deserialize[Any](ByteBuffer.wrap(rs._2))))
        }
      }
    } catch {
      case e: InterruptedException => logger.error("RedisCacheManager.getMultiCacheByKeys－线程中断！", e.getStackTrace())
      case e1: ExecutionException => logger.error("RedisCacheManager.getMultiCacheByKeys－Execution异常！", e1.getStackTrace())
      case e2: Exception => logger.error("RedisCacheManager.getMultiCacheByKeys－Exception异常！", e2.getStackTrace())
    }
    println("keys length " + keys.size + " getMultiCacheByKeys codis action cost: " + (System.currentTimeMillis() - start) + " ms")

    resultMap
  }

  override def hgetall(keys: List[String],
    hgetAllService: java.util.concurrent.ExecutorCompletionService[Seq[(String, java.util.Map[String, String])]]): Map[String, Map[String, String]] = {
    val resultMap = Map[String, Map[String, String]]()
    val start = System.currentTimeMillis();
    // 把原数据划分小批次多线程查询
    val keySplit = keys.sliding(miniBatch, miniBatch).toList
    keySplit.map(keyList => hgetAllService.submit(new QryHashall(keyList)))

    for (index <- 0 until keySplit.size) {
      try {
        // 把查询的结果集放入multimap
        val result = hgetAllService.take().get()
        if (result != null) result.foreach(rs => resultMap += (rs._1 -> rs._2))
      } catch {
        case e: InterruptedException => logger.error("RedisCacheManager.hgetall－线程中断！", e.getStackTrace())
        case e1: ExecutionException => logger.error("RedisCacheManager.hgetall－Execution异常！", e1.getStackTrace())
        case e2: Exception => logger.error("RedisCacheManager.hgetall－Exception异常！", e2.getStackTrace())
      }
    }
    println("keys length " + keys.size + " hgetall codis cost: " + (System.currentTimeMillis() - start) + " ms")
    resultMap
  }

  def hmset(keyValues: Map[String, Map[String, String]]) = keyValues.sliding(miniBatch, miniBatch).toList.foreach(limitBatch =>
    CacheQryThreadPool.threadPool.submit(new FutureTask[String](new InsertHash(limitBatch))))

  override def setCommonCacheValue(cacheName: String, key: String, value: String) = getConnection.hset(cacheName, key, value)
  // added by surq at 2015.12.7 end-----------------UPDATA-------------
}