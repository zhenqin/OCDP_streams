package com.asiainfo.ocdp.stream.service

import java.util.concurrent.FutureTask
import com.asiainfo.ocdp.stream.common.Logging
import com.asiainfo.ocdp.stream.config.MainFrameConf
import com.asiainfo.ocdp.stream.tools.{ CacheQryThreadPool, InsertEventRows, QryEventCache }
import scala.collection.mutable.Map
import scala.collection.immutable
import java.util.concurrent.ExecutorCompletionService
import scala.collection.mutable.ArrayBuffer

/**
 * Created by surq on 12/09/15.
 */
class EventServer extends Logging with Serializable {

  //  //保存事件缓存
  //  def cacheEventData(keyEventIdData: Array[(String, String, String)]) {
  //    val t1 = System.currentTimeMillis()
  //    val miniBatch = MainFrameConf.systemProps.getInt("cacheQryTaskSizeLimit")
  //    val taskMap = Map[Int, FutureTask[String]]()
  //    var index = 0
  //    var innermap = keyEventIdData
  //    while (innermap.size > 0) {
  //      val settask = new InsertEventRows(innermap.take(miniBatch))
  //      val futuretask = new FutureTask[String](settask)
  //      CacheQryThreadPool.threadPool.submit(futuretask)
  //      taskMap.put(index, futuretask)
  //      innermap = innermap.drop(miniBatch)
  //      index += 1
  //    }
  //
  //    println("start thread : " + index)
  //
  //    while (taskMap.size > 0) {
  //      val keys = taskMap.keys
  //      keys.foreach(key => {
  //        val task = taskMap.get(key).get
  //        if (task.isDone) {
  //          taskMap.remove(key)
  //        }
  //      })
  //    }
  //
  //    System.out.println("setEventData " + keyEventIdData.size + " key cost " + (System.currentTimeMillis() - t1))
  //  }

  //  val miniBatch = MainFrameConf.systemProps.getInt("cacheQryTaskSizeLimit")
  //保存事件缓存
  def cacheEventData(keyEventIdData: Array[(String, String, String)]) =
    CacheQryThreadPool.threadPool.execute(new InsertEventRows(keyEventIdData))

  /**
   * 批量读取指定keys的事件缓存
   * batchList[Array:(eventCache:eventKeyValue,jsonValue)]
   */
  def getEventCache(eventCacheService:ExecutorCompletionService[immutable.Map[String, (String, Array[Byte])]],
      batchList: Array[Array[(String, String)]], eventId: String, interval: Int): List[String] = {
    import scala.collection.JavaConversions
    // 满足周期输出的key 和json 。outPutJsonMap :Map[key->json]
    val outPutJsonMap = Map[String, String]()
    batchList.foreach(batch => eventCacheService.submit(new QryEventCache(batch, eventId)))

    // 遍历各batch线程的结果返回值
    for (index <- 0 until batchList.size) {
      // 把查询的结果集放入multimap
      //result: Map[rowKeyList->Tuple2(jsonList->result)]
      val result = eventCacheService.take.get
      val updateArrayBuffer = new ArrayBuffer[(String, String, String)]()
      if (result != null && result.size > 0) {
        result.foreach(rs => {
          // unkey
          val key = rs._1
          val jsonCache = rs._2
          // json 字段
          val json = jsonCache._1
          // codis 中存储的上次营销时间的二进制
          val cache = jsonCache._2
          // 往次营销时间
          val cache_time = if (cache != null) new String(cache) else "0"
          val current_time = System.currentTimeMillis
          // 满足营销
          if (current_time >= (cache_time.toLong + interval * 1000)) {
            // 放入更新codis list等待更新
            updateArrayBuffer.append((key, eventId, String.valueOf(current_time)))
            // 放入输入map等待输出
            outPutJsonMap += (key -> json)
          }
        })
        // 一个batch的数据完成后，更新codis营销时间
        if (updateArrayBuffer.size > 0) cacheEventData(updateArrayBuffer.toArray)
      }
    }
    // 返回所有batchLimt的满足营销时间的数据json
    outPutJsonMap.toList.map(_._2)
  }

  //  //批量读取指定keys的事件缓存
  //  def getEventCache(keys: Array[(String, Array[String])]): Map[String, Map[String, String]] = {
  //    val multimap = Map[String, Map[String, String]]()
  //    var rowKeyAndFields = keys
  //
  //    val miniBatch = MainFrameConf.systemProps.getInt("cacheQryTaskSizeLimit")
  //
  //    val taskMap = Map[Int, FutureTask[Map[String, Map[String, String]]]]()
  //    var index = 0
  //    while (rowKeyAndFields.size > 0) {
  //      val qrytask = new QryEventCache(rowKeyAndFields.take(miniBatch))
  //      val futuretask = new FutureTask[Map[String, Map[String, String]]](qrytask)
  //      CacheQryThreadPool.threadPool.submit(futuretask)
  //      taskMap.put(index, futuretask)
  //
  //      rowKeyAndFields = rowKeyAndFields.drop(miniBatch)
  //      index += 1
  //    }
  //
  //    val cachedata = Map[Int, Map[String, Map[String, String]]]()
  //
  //    println("hgetall start thread : " + index)
  //
  //    var errorFlag = false
  //    while (taskMap.size > 0) {
  //      val keys = taskMap.keys
  //      keys.foreach(key => {
  //        val task = taskMap.get(key).get
  //        if (task.isDone) {
  //          try {
  //            cachedata += (key -> task.get())
  //          } catch {
  //            case e: Exception => {
  //              logError("= = " * 15 + "found error in  RedisCacheManager.getMultiCacheByKeys")
  //              errorFlag = true
  //              e.printStackTrace()
  //            }
  //          } finally {
  //            taskMap.remove(key)
  //          }
  //        }
  //      })
  //    }
  //
  //    cachedata.map(_._2).foreach(resultPerTask => {
  //      resultPerTask.foreach { case (rowKey, fieldValueMap) =>
  //        if (!multimap.contains(rowKey)) {
  //          multimap.put(rowKey, Map[String, String]())
  //        }
  //        fieldValueMap.map { case (field, value) =>
  ////          println("field = " + field + ", value is null or not =" + (value == null))
  //          multimap.get(rowKey).get.put(field, value)
  //        }
  //      }
  //    })
  //
  //    multimap
  //  }

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
