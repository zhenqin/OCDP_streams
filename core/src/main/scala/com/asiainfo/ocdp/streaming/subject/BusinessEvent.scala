/*
package com.asiainfo.ocdp.streaming.business

import com.asiainfo.ocdp.streaming.common.CodisCacheManager
import com.asiainfo.ocdp.streaming.config.{BusinessEventConf, MainFrameConf}
import com.asiainfo.ocdp.streaming.constant.EventConstant
import com.asiainfo.ocdp.streaming.tools.{CacheFactory, DateFormatUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class BusinessEvent extends Serializable with org.apache.spark.Logging {

//  val EVENTCACHE_FIELD_TIME_PREFIX_KEY = "Time:"
//  val EVENTCACHE_FIELD_TIMEBEID_PREFIX_KEY = "Time:beid:"
//  val EVENTCACHE_FIELD_TIMEEVENTID_PREFIX_KEY = "Time:eventId:"
//  val EVENTCACHE_FIELD_EVENTID_PREFIXKEY = "Row:eventId:"

  val DEFAULT_DATETIME_PATTERN = "yyyyMMdd HH:mm:ss.SSS"

  var id: String = null
  var conf: BusinessEventConf = null
  var eventSources: Seq[String] = null
  var eventRules: Seq[String] = null
//  var selectExp: Seq[String] = null
  var interval: Long = EventConstant.DEFAULTINTERVAL
  var delayTime: Long = EventConstant.DEFAULTDELAYTIME
  val locktime: String = EventConstant.LOCKTIMEFIELD
  var userKeyColumnName: String = _
  var currentDiid: String = _
	var schema: StructType = _

	var eventId2SelectExprMap: mutable.Map[String, String] = _

//	val streamingOutputWriter = new StreamingOutputWriter

  def getDelim: String = conf.get("delim")
  
  def getTimeStr(cache: mutable.Map[String, String], key: String): String = cache.getOrElse(key, "0")

	//从原始row(select 之前的row)重获取event缓存的key取值
	def getEventCacheKey(row: Row, selectSchema: StructType, eventId: String): String =
		EventConstant.EVENT_CACHE_PREFIX_NAME + MainFrameConf.diid2DataInterfaceConfMap.get(currentDiid).get.get("uniqKeys").split(",").map(fieldName => {
			row.getAs[String](fieldName)
		}).mkString(MainFrameConf.diid2DataInterfaceConfMap.get(currentDiid).get.get("UKSeparator"))

  def init(diid: String, beconf: BusinessEventConf) {
    currentDiid = diid
    conf = beconf
    id = conf.get("beid")
    interval = conf.getLong("interval", EventConstant.DEFAULTINTERVAL) //营销周期
    delayTime = conf.getLong("delaytime", EventConstant.DEFAULTDELAYTIME) //
    userKeyColumnName = conf.get("userKeyColumnName") //selectExp中用户唯一标识字段名

    eventId2SelectExprMap =  MainFrameConf.beid2EventId2SelectExprMap.get(id).get

	  schema = MainFrameConf.diid2SchemaMap.get(currentDiid).get
  }

  /**
   * (业务或主题)订阅事件
   * @param eventMap 单元事件与满足事件条件的数据集
   */
  def subcribeEvent(eventMap: mutable.Map[String, DataFrame]) {
    //仅获取指定业务的关联的事件信息 Map[eventId, DF]
    val filtevents = eventMap.filter(x => MainFrameConf.beid2EventIdsMap.get(id).get.contains(x._1))
    val (currentEventRuleId, currentEvent) = filtevents.iterator.next() //约定配置模式：1个业务在1个流上只配置一个eventRules

    val currentEventSourceId = MainFrameConf.eventId2DIidMap.get(currentEventRuleId).get

//    val selectedData = currentEvent.selectExpr(selectExp: _*)
    val currentEventSelectExpr = MainFrameConf.beid2EventId2SelectExprMap.get(id).get.get(currentEventRuleId).get

    val selectColumnsArr = currentEventSelectExpr.split(",")
    val selectedData = currentEvent.selectExpr(selectColumnsArr: _*)
	  val selectSchema = selectedData.schema

	  val eventUserKeyCacheKeyData = currentEvent.rdd.map(row=>{
		  (getFieldOnRow(row, userKeyColumnName), getEventCacheKey(row, schema, currentEventRuleId))
	  })
	  val rddUserKeyCachekeyRow = eventUserKeyCacheKeyData.zip(selectedData.rdd).map(x=>(x._1._1, x._1._2, x._2))

    // println("* * " * 20 +"currentEventRuleId = " + currentEventRuleId +", selectedData = ")
    // selectedData.show()
    // println("= = " * 20 +"currentEventRuleId = " + currentEventRuleId +", selectedData done")
    
//	  val rddRow = transformDF2RDD(selectedData, userKeyIdx)
	  val rddCacheKeyRow = RDDRePartitionByKey(rddUserKeyCachekeyRow)

    rddCacheKeyRow.mapPartitions(iter=>{
      //遍历每个分区，批量查询外部缓存中存储的业务/主题的缓存
      //之后判断业务订阅的事件规则和业务输出规则是否都满足，满足则进行输出
      //  输出时如果存在多个事件属性关联，则根据key获取其他事件相关的cache，取出需要的信息
      //  并且按顺序排列输出
      new Iterator[Tuple2[String, Row]]{
        private[this] var current: Tuple2[String, Row] = _
        private[this] var currentPos: Int = -1
//        private[this] var batchArray: Array[Row] = _
        private[this] val batchArrayBuffer = new ArrayBuffer[Tuple2[String, Row]]()
        private[this] val beOutputArrs = ArrayBuffer[ArrayBuffer[(String, String)]]()

        override def hasNext: Boolean ={
          iter.hasNext && batchNext()
        }

        override def next(): Tuple2[String, Row] ={
          batchArrayBuffer.head
        }

        var numBatches = 0
        var batchSize = 0
        val batchLimit = MainFrameConf.systemProps.getInt("cacheQryBatchSizeLimit")

        //批量处理
        def batchNext(): Boolean ={
          var result = false
          batchArrayBuffer.clear()

          //构造批量
          while (iter.hasNext && (batchSize < batchLimit)) {
            current = iter.next()
            batchArrayBuffer.append(current)

            batchSize += 1
            currentPos += 1
          }

          if(batchArrayBuffer.length > 0) {
//            batchArray = batchArrayBuffer.toArray
            result = true

            // 构造批量查询的key信息，格式Array[(rowKey, Array[String])； 查询返回 Map[String, Map[String, Any]]
//            val qryKeys = batchArrayBuffer.map(getHashKey(_)) //获取1个批次(此处的批次与spark-streaming的batch不同)需要查询业务订阅缓存的keys
            val qryEventCacheKeys = batchArrayBuffer.map(_._1)
            val qryKeys = qryEventCacheKeys.map(eventCacheKey=>{
	            val fields = MainFrameConf.beid2EventIdsMap.get(id).get.flatMap(eventId => {
		            Array(EventConstant.EVENTCACHE_FIELD_ROWEVENTID_PREFIX_KEY + eventId,
			            EventConstant.EVENTCACHE_FIELD_TIMEEVENTID_PREFIX_KEY + eventId)
	            })
              fields.append(EventConstant.EVENTCACHE_FIELD_TIMEBEID_PREFIX_KEY + id)
              (eventCacheKey, fields.toArray)
            })
            
//            val businessCache = CacheFactory.getManager.hgetall(qryEventCacheKeys.toList) //批次获取需要的业务订阅缓存
            val businessCache = CacheFactory.getManager.asInstanceOf[CodisCacheManager].getEventCache(qryKeys.toArray)
            val needUpdatedCache = mutable.Map[String, mutable.Map[String, String]]()

            for(cacheKeyRow <- batchArrayBuffer){
              filterBSEvent(cacheKeyRow._2, cacheKeyRow._1, currentEventSourceId, currentEventRuleId,
                businessCache, needUpdatedCache,
//                outputRowsKeySet,
                beOutputArrs)
            }

            //更新的事件时间cache
            val t2 = System.currentTimeMillis()
            CacheFactory.getManager.hmset(needUpdatedCache)

            println(" update saled user data cost time : " + (System.currentTimeMillis() - t2) + " millis ! ")

            //输出
            if(beOutputArrs.length > 0) {
              logInfo("batchSize = " + batchSize+ ", outputRows.length = " + beOutputArrs.length +", numFiltered = " + (batchSize - beOutputArrs.length))

              StreamingOutputWriter.output(beOutputArrs.map(_.toArray).toArray, id)

              //输出后清空 beOutputArrs
              beOutputArrs.clear()
            }

            batchSize = 0
            numBatches += 1
          }
          result
        }
      }
    }).count()

  }

  /**
   * 业务事件排重（周期营销），更新业务缓存
   * @param row
   * @param old_cache
   * @param needUpdatedCache 存储需要被更新的cache
//   * @param outputRowsKeySet 用于判断是否存在重复记录
   */
  def filterBSEvent(row: Row, cacheKey: String, diid: String, erid: String,
                    old_cache: mutable.Map[String, mutable.Map[String, Any]],
                    needUpdatedCache: mutable.Map[String, mutable.Map[String, String]],
//                  outputRowsKeySet: scala.collection.mutable.Set[String], //
                    beOutputRows: ArrayBuffer[ArrayBuffer[(String,String)]]): Unit = {
    val currentSystemTimeMs = System.currentTimeMillis()
    //    val qryKey = getHashKey(row)
//    val qryKey = getEventCacheKey(row, selectSchema, erid)
	  val qryKey = cacheKey

	  //    var keyCache = old_cache.getOrElse(qryKey, mutable.Map[String, Any]())
    val keyCacheAboutRow = old_cache.getOrElse(qryKey, mutable.Map[String, Any]())
            .filter(_._1.startsWith(EventConstant.EVENTCACHE_FIELD_ROWEVENTID_PREFIX_KEY))
            .map { case (k, v) => (k, v.asInstanceOf[Row]) }
    val keyCacheAboutTimeStr = old_cache.getOrElse(qryKey, mutable.Map[String, Any]())
            .filter(_._1.startsWith(EventConstant.EVENTCACHE_FIELD_TIME_PREFIX_KEY))
            .map { case (k, v) => (k, v.asInstanceOf[String]) }
    //    val timeStr = getTime(row)
    //TODOx: 业务事件缓存中的事件满足时间，取日志中的时间还是分析时的系统时间？是否提供配置实现可选？
    //      配置 eventActiveTimeMs = if (选日志时间) timeStr else currentSystemTimeMs

    val timeCacheMap = mutable.Map[String, String]()
    val events = MainFrameConf.beid2EventIdsMap.get(id).get



    var flagTriggerBSEvent = false
      //Note: 指定rowKey的keyCache是按需读取的，包含的信息有： eventId->Row, beid->lastActiveTimeMs timePrefix:eventId->lastActiveTimeMs

      val timeBeidField = EventConstant.EVENTCACHE_FIELD_TIMEBEID_PREFIX_KEY + id
      val lastBSActiveTimeStr = getTimeStr(keyCacheAboutTimeStr, timeBeidField)

      val timeEventIdField = EventConstant.EVENTCACHE_FIELD_TIMEEVENTID_PREFIX_KEY + erid
      val lastEventActiveTimeStr = getTimeStr(keyCacheAboutTimeStr, timeEventIdField)

      //无论业务事件是否满足，事件时间都要更新
      timeCacheMap.put(timeEventIdField, DateFormatUtils.dateMs2Str(currentSystemTimeMs, DEFAULT_DATETIME_PATTERN))

      if (keyCacheAboutTimeStr.size == 0) {
        //cache为空(存储event Rows时不存储time)，说明业务事件没有触发过；此时要判断是否所有事件都满足(激活)
        //    1.1如果激活，更新业务和事件时间;
        //    1.2没有激活，仅更新事件时间
        if (events.size == 1) {
          //业务关联的仅关联1个事件，业务满足触发
          
          timeCacheMap.put(timeBeidField, DateFormatUtils.dateMs2Str(currentSystemTimeMs, DEFAULT_DATETIME_PATTERN))
          flagTriggerBSEvent = true
          
        } else {
          //events.size > 1，只有部分事件满足，业务事件没有触发过
//          flag = false
        }
      } else {
        // cache不为空
        // 检查业务触发时间，
        //  1如果不存在，说明业务事件没有触发过；此时要判断是否所有事件都满足(激活)
        //    1.1如果激活，更新业务和该事件时间;
        //    1.2没有激活，仅更新事件时间
        //  2如果存在，寿命业务事件处罚过，此时要判断是否满足业务事件触发周期/黑名单等
        //      val lastBSActiveTimeMs = keyCacheAboutTimeStr.getOrElse(get, "0").toLong
        if (lastBSActiveTimeStr == "0") { //
          if (keyCacheAboutTimeStr.size == events.size) {
            //业务关联的所有事件都满足
            timeCacheMap.put(timeBeidField, DateFormatUtils.dateMs2Str(currentSystemTimeMs, DEFAULT_DATETIME_PATTERN))
            flagTriggerBSEvent = true
            
          } else {
            //业务关联事件没有都满足
//            flag = false
          }
        } else {
          // lastBSActiveTimeStr != "0"是个有效的时间，说明业务事件已经触发过，是否要判断当前时间是否满足业务事件周期
          //TODO: 需要1增加日志发生事件与当前事件发生时间的比较，过滤乱序日志事件；

//          val lastBSActiveTimeMs = DateFormatUtils.dateStr2Ms(lastBSActiveTimeStr, DEFAULT_DATETIME_PATTERN)
          val lastBSActiveTimeMs = transformDateStr2Ms(lastBSActiveTimeStr, DEFAULT_DATETIME_PATTERN) //转换失败的异常处理
          if(lastBSActiveTimeMs == 0){ //业务发生时间不对的情况下，重置但并不触发
            timeCacheMap.put(timeBeidField, DateFormatUtils.dateMs2Str(currentSystemTimeMs, DEFAULT_DATETIME_PATTERN))
//            flag = false
          } else {
            if (lastBSActiveTimeMs + interval > currentSystemTimeMs) {
              // 当前事件生成时间（与最近一次触发事件的时间相比）在周期策略内，不触发
//              flag = false
            } else {
              // 当前事件生成时间（与最近一次触发事件的时间相比）在周期策略外，可以重新触发
              timeCacheMap.put(timeBeidField, DateFormatUtils.dateMs2Str(currentSystemTimeMs, DEFAULT_DATETIME_PATTERN))
	            flagTriggerBSEvent = true
            }
          }
        }
      }

	  //多流延迟过滤规则
	  if(events.size > 1){ //业务只关联1个事件时不需要多流延迟过滤
		  //业务有多个事件，取其他事件的最早的发生时间，
	    if(flagTriggerBSEvent) { //在业务周期规则上，比较多个流上事件发生的时间，实现多流延迟的过滤功能

		    val otherEventCacheTimeMs = events.filterNot(_ == erid).map(erid_tmp => {
			    val timeEventIdField = getTimeStr(keyCacheAboutTimeStr, EventConstant.EVENTCACHE_FIELD_TIMEEVENTID_PREFIX_KEY + erid_tmp)
			    keyCacheAboutTimeStr.get(timeEventIdField) match {
				    case Some(timeStr) => (erid_tmp, DateFormatUtils.dateStr2Ms(timeStr, DEFAULT_DATETIME_PATTERN))
				    case None => (erid_tmp, 0L)
			    }
		    }).filterNot(_._2 == 0) //如果事件发生时间为0，表明还没有发生过

		    //根据时间发生时间升序排序，取最小值与当前事件发生事件比较
		    if (otherEventCacheTimeMs.length > 0) { //otherEventCacheTimeMs ==0 的情况和 (events.size >1 && flagTriggerBSEvent) 不会同时发生
			    val minEventTimeMs = otherEventCacheTimeMs.sortBy(_._2).head._2
			    if (currentSystemTimeMs - minEventTimeMs > delayTime) flagTriggerBSEvent = false
		    }
	    }
	  }
	  needUpdatedCache.put(qryKey, timeCacheMap)
    
    if(flagTriggerBSEvent){
      //业务事件触发时，构造输出字段
      beOutputRows.append(getBEOutputArr(row, erid, keyCacheAboutRow, id))
    }
  }


  //业务关联所有事件满足时构造输出的内容
  /**
   *
   * @param row //应用selectExpr后的字段
   * @param erid //当前row对应事件id
   * @param eventRowCache //应用selectExpr前的字段
   * @param beid
   * @return (fieldName, Value)元组的有序数组
   */
  def getBEOutputArr(row: Row, erid: String,
                  eventRowCache: mutable.Map[String, Row],
                  beid: String): ArrayBuffer[(String, String)] = {

    val eventid2SelectExprArr = MainFrameConf.beid2EventIdSelectExprArrMap.get(beid).get
    if(eventid2SelectExprArr.length == 1){
      val selectExprArr = eventid2SelectExprArr.head._2.split(",")
      val result = ArrayBuffer[(String, String)]()
      selectExprArr.zipWithIndex.foreach{ case (fieldName, idx)=>
        result.append((fieldName, row.getAs[String](idx)))
      }
      result
    } else {
      //构造事件id与selectExpr中字段的顺序，确保不同数据接口分析应用进程对同一业务的输出的字段顺序一致
      val eventId2SelectExprSortedArr = eventid2SelectExprArr.sortBy(_._1)
      val diid2Schema = MainFrameConf.diid2SchemaMap
      val eventId2Schema = MainFrameConf.eventId2DIidMap.map{case (k, v) =>
        (k, MainFrameConf.diid2SchemaMap.get(v).get)
      }
	    
      val result = eventId2SelectExprSortedArr.map{case (eventId, selectExpr)=>
        val rowKey = EventConstant.EVENTCACHE_FIELD_ROWEVENTID_PREFIX_KEY + eventId
        val row = eventRowCache.get(rowKey).get
        val selectFieldValueArr = selectExpr.split(",").map(fieldName=>{
	        (fieldName, getFieldOnRow(row, fieldName))
        })
        selectFieldValueArr
      }

      result.flatMap(x=>x)
    }
  }
	
	//支持获取Row中 labels: Map[String, Map[String, String]]的字段
	def getFieldOnRow(row: Row, fieldName: String): String = {
		val LABEL_PATTERN = """labels\['(.*)'\]\['(.*)'\]""".r
		if (fieldName.startsWith("labels")) {
			val LABEL_PATTERN(k1, k2) = fieldName
			row.getAs[mutable.Map[String, mutable.Map[String, String]]](row.length-1).get(k1).get(k2)
		} else {
			row.getAs[String](fieldName)
		}
	}

  //转换失败的异常处理
  def transformDateStr2Ms(timeStr: String, dateTimePattern: String): Long = {
    try{
      DateFormatUtils.dateStr2Ms(timeStr, dateTimePattern)
    } catch {
      case ex: java.text.ParseException => //时间字段转换时出现解析异常，认为时间字段无效
        0L
      case ex: Exception => //时间字段转换时出现其他异常，抛出异常
        throw ex
    }
  }

  /**
   * 更新业务缓存中指定事件的最新满足时间，目前配置模式：一个事件流配置一个事件
   * @param row
   * @param keyCacheAboutTimeStr
   * @param diid
   * @param activeSystemTimeMs
   */
  def updateEventActiveTime(row: Row, keyCacheAboutTimeStr: mutable.Map[String, String],
                            keyCacheAboutRow: mutable.Map[String, Row],
                            diid: String,
                            activeSystemTimeMs: Long = System.currentTimeMillis()): Unit ={

    val lastEventActiveTimeStr = keyCacheAboutTimeStr.get(diid)
    val lastEventActiveTimeMs =  lastEventActiveTimeStr match {
      case Some(timeStr) =>
        try{
          DateFormatUtils.dateStr2Ms(timeStr, "yyyyMMdd HH:mm:ss.SSS")
        } catch {
          case ex: java.text.ParseException => //时间字段转换时出现解析异常，认为时间字段无效
            0L
          case ex: Exception => //时间字段转换时出现其他异常，抛出异常
            throw ex
        }
      case None =>
        0L
    }

    if (lastEventActiveTimeMs < activeSystemTimeMs) {
      keyCacheAboutTimeStr.put(diid, DateFormatUtils.dateMs2Str(activeSystemTimeMs, "yyyyMMdd HH:mm:ss.SSS"))
    }
  }
  
  

  //  def output(data: RDD[Option[Row]])

  //如果需要根据指定的key进行分组，可以自定义业务类，extends BusinessEvent 通用类，overwrite transformDF2RDD 方法
  def transformDF2RDD(old_dataframe: DataFrame, partitionKeyIdx: Int): RDD[Row] = old_dataframe.rdd

	//rddUserKeyCacheKeyRow=>rddCacheKeyRow(可以重写根据指定的key分区)
	def RDDRePartitionByKey(rdd: RDD[(String, String, Row)]): RDD[(String, Row)] = rdd.map(row=>{
		(row._2, row._3)
	})
}
*/
