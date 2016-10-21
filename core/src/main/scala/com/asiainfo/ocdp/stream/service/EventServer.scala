package com.asiainfo.ocdp.stream.service

import java.util.concurrent.atomic.AtomicInteger

import com.asiainfo.ocdp.stream.common.Logging
import com.asiainfo.ocdp.stream.tools.{ CacheQryThreadPool, InsertEventRows, QryEventCache }
import scala.collection.mutable.Map
import scala.collection.immutable
import java.util.concurrent.ExecutorCompletionService
import scala.collection.mutable.ArrayBuffer

/**
 * Created by surq on 12/09/15.
 */
class EventServer extends Logging with Serializable {


  //  val miniBatch = MainFrameConf.systemProps.getInt("cacheQryTaskSizeLimit")
  //保存事件缓存
  def cacheEventData(keyEventIdData: Array[(String, String, String)]) =
    CacheQryThreadPool.threadPool.execute(new InsertEventRows(keyEventIdData))

  /**
    * * 批量读取指定keys的事件缓存
    * batchList[Array:(eventCache:eventKeyValue,jsonValue)]
    *
    * @param eventCacheService    线程池
    * @param batchList             需要放入 codis 的 keyList
    * @param eventId               事件类型ID
    * @param interval              营销周期
    * @return
    */
  def getEventCache(eventCacheService:ExecutorCompletionService[immutable.Map[String, (String, Array[Byte])]],
      batchList: Array[Array[(String, String)]], eventId: String, interval: Int): List[String] = {
    import scala.collection.JavaConversions
    // 满足周期输出的key 和json 。outPutJsonMap :Map[key->json]
    val outPutJsonMap = Map[String, String]()
    batchList.foreach(batch => eventCacheService.submit(new QryEventCache(batch, eventId)))

    logInfo("batchList size " + batchList.size + " event: " + eventId + " interval: " + interval)
    var printCount = new AtomicInteger(0)
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
          //modify zhenqin，刘欢。 原1000改变为1000L，Int 值过大溢出，换为 Long 类型
          //if (java.lang.Boolean.parseBoolean("false")) {
          if(current_time >= (cache_time.toLong + interval * 1000L) && !outPutJsonMap.contains(key)) {
            logInfo(key + " 上次营销: " + cache_time.toLong)
            printCount.incrementAndGet()

            // 放入更新codis list等待更新
            updateArrayBuffer.append((key, eventId, String.valueOf(current_time)))
            // 放入输入map等待输出
            outPutJsonMap += (key -> json)
          }
        })
        // 一个batch的数据完成后，更新codis营销时间
        if (updateArrayBuffer.nonEmpty) {
          cacheEventData(updateArrayBuffer.toArray)
        }
      }
    }
    logInfo("本批次： " + batchList.size + " 营销数据: " + printCount.get())
    // 返回所有batchLimt的满足营销时间的数据json
    outPutJsonMap.toList.map(_._2)
  }
}
