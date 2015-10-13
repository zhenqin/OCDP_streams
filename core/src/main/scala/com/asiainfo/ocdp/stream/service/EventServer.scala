package com.asiainfo.ocdp.stream.service

import java.util.concurrent.FutureTask

import com.asiainfo.ocdp.stream.common.Logging
import com.asiainfo.ocdp.stream.config.MainFrameConf
import com.asiainfo.ocdp.stream.tools.{CacheQryThreadPool, InsertEventRows, QryEventCache}

import scala.collection.mutable.Map

/**
 * Created by leo on 9/29/15.
 */
class EventServer extends Logging with Serializable {

  //保存事件缓存
  def cacheEventData(keyEventIdData: Array[(String, String, String)]) {
    val t1 = System.currentTimeMillis()
    val miniBatch = MainFrameConf.systemProps.getInt("cacheQryTaskSizeLimit")
    val taskMap = Map[Int, FutureTask[String]]()
    var index = 0
    var innermap = keyEventIdData
    while (innermap.size > 0) {
      val settask = new InsertEventRows(innermap.take(miniBatch))
      val futuretask = new FutureTask[String](settask)
      CacheQryThreadPool.threadPool.submit(futuretask)
      taskMap.put(index, futuretask)
      innermap = innermap.drop(miniBatch)
      index += 1
    }

    println("start thread : " + index)

    while (taskMap.size > 0) {
      val keys = taskMap.keys
      keys.foreach(key => {
        val task = taskMap.get(key).get
        if (task.isDone) {
          taskMap.remove(key)
        }
      })
    }

    System.out.println("setEventData " + keyEventIdData.size + " key cost " + (System.currentTimeMillis() - t1))
  }

  //批量读取指定keys的事件缓存
  def getEventCache(keys: Array[(String, Array[String])]): Map[String, Map[String, String]] = {
    val multimap = Map[String, Map[String, String]]()
    var rowKeyAndFields = keys

    val miniBatch = MainFrameConf.systemProps.getInt("cacheQryTaskSizeLimit")

    val taskMap = Map[Int, FutureTask[Map[String, Map[String, String]]]]()
    var index = 0
    while (rowKeyAndFields.size > 0) {
      val qrytask = new QryEventCache(rowKeyAndFields.take(miniBatch))
      val futuretask = new FutureTask[Map[String, Map[String, String]]](qrytask)
      CacheQryThreadPool.threadPool.submit(futuretask)
      taskMap.put(index, futuretask)

      rowKeyAndFields = rowKeyAndFields.drop(miniBatch)
      index += 1
    }

    val cachedata = Map[Int, Map[String, Map[String, String]]]()

    println("hgetall start thread : " + index)

    var errorFlag = false
    while (taskMap.size > 0) {
      val keys = taskMap.keys
      keys.foreach(key => {
        val task = taskMap.get(key).get
        if (task.isDone) {
          try {
            cachedata += (key -> task.get())
          } catch {
            case e: Exception => {
              logError("= = " * 15 + "found error in  RedisCacheManager.getMultiCacheByKeys")
              errorFlag = true
              e.printStackTrace()
            }
          } finally {
            taskMap.remove(key)
          }
        }
      })
    }

    cachedata.map(_._2).foreach(resultPerTask => {
      resultPerTask.foreach { case (rowKey, fieldValueMap) =>
        if (!multimap.contains(rowKey)) {
          multimap.put(rowKey, Map[String, String]())
        }
        fieldValueMap.map { case (field, value) =>
          println("field = " + field + ", value is null or not =" + (value == null))
          multimap.get(rowKey).get.put(field, value)
        }
      }
    })

    multimap
  }

  //批量读取指定keys的事件缓存
  /*def getAllEventCache(keys: scala.collection.mutable.Set[String]): Map[String, Map[String, Any]] = {
    val multimap = Map[String, Map[String, Any]]()
    var rowKeyAndFields = keys

    val miniBatch = MainFrameConf.systemProps.getInt("cacheQryTaskSizeLimit")

    val taskMap = Map[Int, FutureTask[Map[String, Map[String, Array[Byte]]]]]()
    var index = 0
    while (rowKeyAndFields.size > 0) {
      val qrytask = new QryAllEventCache(rowKeyAndFields.take(miniBatch))
      val futuretask = new FutureTask[Map[String, Map[String, Array[Byte]]]](qrytask)
      CacheQryThreadPool.threadPool.submit(futuretask)
      taskMap.put(index, futuretask)

      rowKeyAndFields = rowKeyAndFields.drop(miniBatch)
      index += 1
    }

    val cachedata = Map[Int, Map[String, Map[String, Array[Byte]]]]()

    println("hgetall start thread : " + index)

    var errorFlag = false
    while (taskMap.size > 0) {
      val keys = taskMap.keys
      keys.foreach(key => {
        val task = taskMap.get(key).get
        if (task.isDone) {
          try {
            cachedata += (key -> task.get())
          } catch {
            case e: Exception => {
              logError("= = " * 15 + "found error in  RedisCacheManager.getMultiCacheByKeys")
              errorFlag = true
              e.printStackTrace()
            }
          } finally {
            taskMap.remove(key)
          }
        }
      })
    }

    cachedata.map(_._2).foreach(resultPerTask => {
      resultPerTask.foreach { case (rowKey, fieldValueMap) =>
        multimap.put(rowKey, fieldValueMap.map { case (f, v) =>
          (f, getKryoTool.deserialize[Any](ByteBuffer.wrap(v)))
        })
      }
    })
    multimap
  }*/
}
