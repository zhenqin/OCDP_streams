package streaming.label

import com.asiainfo.ocdp.streaming.common.StreamingCache
import com.asiainfo.ocdp.streaming.constant.LabelConstant
import com.asiainfo.ocdp.streaming.tools.DateFormatUtils
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types.StructType

import scala.collection.{immutable, mutable}
import scala.collection.mutable.Map
import scala.util.Sorting._

/**
 * Created by tsingfu on 15/9/15.
 */
class LocationStay extends MCLabel {

	// 获取配置 用户设定的各业务连续停留的时间坎(排序升序)
//	lazy val stayTimeThresholdList = conf.get(LabelConstant.STAY_LIMITS).split(LabelConstant.ITME_SPLIT_MARK).map(_.trim)
	lazy val stayTimeOrderList = conf.get(LabelConstant.STAY_LIMITS).split(LabelConstant.ITME_SPLIT_MARK).map(_.trim.toLong).sorted
	// 推送满足设置的数据坎的最大值:true;最小值：false
	lazy val userDefPushOrde = conf.getBoolean(LabelConstant.STAY_MATCHMAX, true)
	// 推送满足设置的数据的限定值，还是真实的累计值.真实的累计值:false;限定值:true
	lazy val pushLimitValue = conf.getBoolean(LabelConstant.STAY_OUTPUTTHRESHOLD, true)
	// 无效数据阈值的设定
	lazy val thresholdValue = conf.getLong(LabelConstant.STAY_TIMEOUT, LabelConstant.DEFAULT_TIMEOUT_VALUE)

	/**
	 * 框架调用入口方法
	 */
	override def attachMCLabel(mcLogRow: GenericMutableRow, schema: StructType, cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): StreamingCache = {

		//1获取标签map
		val lablesMap = mcLogRow.getAs[String](mcLogRow.length - 1).asInstanceOf[Map[String, Map[String, String]]]

		//2更新标签
		//2.1 根据标签缓存和当前日志信息更新标签
		//a. 获取locationStayRule的cache
		val cacheInstance = if (cache == null) new LabelProps else cache.asInstanceOf[LabelProps]

		// cache中各区域的属性map
		val cacheImmutableMap = cacheInstance.labelsPropList
		// map属性转换
		val cacheMutableMap = transformCacheMap2mutableMap(cacheImmutableMap)
		// mcsource 打标签用
		val mcStayLabelsMap = Map[String, String]()

		// cache中所有区域的最大lastTime
		val cacheMaxLastTime = getCacheMaxLastTime(cacheMutableMap)
		// 取在siteRule（区域规则）中所打的area标签list
		val locationList = lablesMap.get(LabelConstant.LABEL_ONSITE).get.keys

		val timeMs = DateFormatUtils.dateStr2Ms(mcLogRow.getAs[String](schema.fieldIndex("time")), "yyyyMMdd HH:mm:ss.SSS")

		locationList.map(location => {
			// A.此用的所有区域在cache中的信息已经过期视为无效，标签打为“0”；重新设定cache;
			if (timeMs - cacheMaxLastTime > thresholdValue) {
				// 1. 连续停留标签置“0”
				mcStayLabelsMap += (location -> LabelConstant.LABEL_STAY_TIME_ZERO)
				// 2. 清除cache信息
				cacheMutableMap.clear()
				// 3. 重置cache信息
				val cacheStayLabelsMap = Map[String, String]()
				cacheStayLabelsMap += (LabelConstant.LABEL_STAY_FIRSTTIME -> timeMs.toString)
				cacheStayLabelsMap += (LabelConstant.LABEL_STAY_LASTTIME -> timeMs.toString)
				cacheMutableMap += (location -> cacheStayLabelsMap)
			} else if (cacheMaxLastTime - timeMs > thresholdValue) {
				//B.此条数据为延迟到达的数据，已超过阈值视为无效，标签打为“0”；cache不做设定;
				// 1. 连续停留标签置“0”
				mcStayLabelsMap += (location -> LabelConstant.LABEL_STAY_TIME_ZERO)
			} else {
				//C.此条数据时间为[(maxLastTime-thresholdValue)~maxLastTime~(maxLastTime+thresholdValue)]之间
				labelAction(location, cacheMutableMap, mcStayLabelsMap, timeMs)
			}
		})

		// c. 给mcsoruce设定连续停留[LABEL_STAY]标签
		lablesMap.put(LabelConstant.LABEL_STAY, mcStayLabelsMap)

		//2.2 根据查询cache和当前日志信息更新标签

		//3更新缓存
		// map属性转换
		cacheInstance.labelsPropList = transformCacheMap2ImmutableMap(cacheMutableMap)
		cacheInstance
	}

	/**
	 * 把cache的数据转为可变map
	 */
	private def transformCacheMap2mutableMap(cacheInfo: scala.collection.immutable.Map[String, scala.collection.immutable.Map[String, String]]) = {
		val labelsPropMap = Map[String, Map[String, String]]()
		cacheInfo.map(infoMap => {
			val copProp = Map[String, String]()
			infoMap._2.foreach(copProp += _)
			labelsPropMap += (infoMap._1 -> copProp)
		})
		labelsPropMap
	}

	/**
	 * 编辑完chache中的内容后重新置为不可变类属
	 */
	private def transformCacheMap2ImmutableMap(labelsPropMap: Map[String, Map[String, String]]) = {
		if (labelsPropMap.isEmpty) immutable.Map[String, immutable.Map[String, String]]() else labelsPropMap.map(propSet => (propSet._1, propSet._2.toMap)).toMap
	}

	/**
	 * 从cache的区域List中取出最大的lastTime<br>
	 */
	private def getCacheMaxLastTime(labelsPropMap: Map[String, Map[String, String]]): Long = {
		val areaPropArray = labelsPropMap.toArray

		if (areaPropArray.isEmpty) 0L
		else {
			// 对用户cache中的区域列表按lastTime排序（升序）
			quickSort(areaPropArray)(Ordering.by(_._2.get(LabelConstant.LABEL_STAY_LASTTIME)))
			// 取用户cache区域列表中的最大lastTime
			areaPropArray.reverse(0)._2.get(LabelConstant.LABEL_STAY_LASTTIME).get.toLong
		}
	}

	/**
	 * 打标签处理并且更新cache<br>
	 */
	private def labelAction(location: String,
	                        labelsPropMap: Map[String, Map[String, String]],
	                        mcStayLabelsMap: Map[String, String],
	                        mcTime: Long) {
		// cache属性map
		//    lazy val labelsPropMap = cacheInstance.labelsPropList
		// b. 使用宽松的过滤策略，相同区域信令如果间隔超过${thresholdValue}，则判定为不连续
		//    val area = labelsPropMap.get(location)
		val area = labelsPropMap.get(location)
		area match {
			case None => {
				mcStayLabelsMap += (location -> LabelConstant.LABEL_STAY_TIME_ZERO)
				addCacheAreaStayTime(labelsPropMap, location, mcTime, mcTime)
			}
			case Some(currentStatus) => {
				val first = getCacheStayTime(currentStatus).get("first").get
				val last = getCacheStayTime(currentStatus).get("last").get
				//        println("FIRST TIME : " + first + " , LAST TIME : " + last + " , MCTIME : " + mcTime)
				if (first > last) {
					// 无效数据，丢弃，本条视为first
					mcStayLabelsMap += (location -> LabelConstant.LABEL_STAY_DEFAULT_TIME)
					updateCacheStayTime(currentStatus, mcTime, mcTime)
				} else if (mcTime < first) {
					// 本条记录属于延迟到达，更新开始时间
					currentStatus.put(LabelConstant.LABEL_STAY_FIRSTTIME, mcTime.toString)
					mcStayLabelsMap.put(location, evaluateTime(last - first, last - mcTime))
					if (first - mcTime > thresholdValue) {
						// 本条记录无效，输出空标签，不更新cache
						mcStayLabelsMap += (location -> LabelConstant.LABEL_STAY_TIME_ZERO)
					} else {
						// 本条记录属于延迟到达，更新开始时间
						currentStatus.put(LabelConstant.LABEL_STAY_FIRSTTIME, mcTime.toString)
						mcStayLabelsMap.put(location, evaluateTime(last - first, last - mcTime))
					}
				} else if (mcTime <= last) {
					// 本条属于延迟到达，不处理
					mcStayLabelsMap += (location -> LabelConstant.LABEL_STAY_DEFAULT_TIME)
				} else if (mcTime - last > thresholdValue) {
					// 本条与上一条数据间隔过大，判定为不连续
					mcStayLabelsMap += (location -> LabelConstant.LABEL_STAY_DEFAULT_TIME)
					updateCacheStayTime(currentStatus, mcTime, mcTime)
				} else {
					// 本条为正常新数据，更新cache后判定
					currentStatus.put(LabelConstant.LABEL_STAY_LASTTIME, mcTime.toString)

					val newtime = evaluateTime(last - first, mcTime - first)
					mcStayLabelsMap.put(location, newtime)
				}
			}
		}
	}

	/**
	 * 根据本次以及前次的停留时间计算出标签停留时间的值
	 */
	private def evaluateTime(oldStayTime: Long, newStayTime: Long): String = {
		// 新状态未达到最小坎时或旧状态超过最大坎值时返回黙认值“0”
		if (newStayTime < stayTimeOrderList(0) ||
			oldStayTime > stayTimeOrderList(stayTimeOrderList.size - 1)) {
			LabelConstant.LABEL_STAY_DEFAULT_TIME
		} else {
			val matchList = stayTimeOrderList.filter(limit => (oldStayTime <= limit && newStayTime >= limit))
			// 新旧停留时间在某坎区间内，返回黙认值“0”
			if (matchList.isEmpty) LabelConstant.LABEL_STAY_DEFAULT_TIME
			// 新旧停留时间为跨坎区域时间，推送设置的数据坎的值
			else if (pushLimitValue) {
				val result = if (userDefPushOrde) matchList.map(_.toLong).max else matchList.map(_.toLong).min
				result.toString
			} // 推送真实数据值
			else newStayTime.toString
		}
	}

	/**
	 * 取cache中的firstTime,lastTime<br>
	 * 返回结果map,key:"first"和"last"<br>
	 */
	private def getCacheStayTime(currentStatus: Map[String, String]) = {
		val first = currentStatus.get(LabelConstant.LABEL_STAY_FIRSTTIME)
		val last = currentStatus.get(LabelConstant.LABEL_STAY_LASTTIME)
		if (first == None || last == None) Map("first" -> 0L, "last" -> 0L)
		else Map("first" -> first.get.toLong, "last" -> last.get.toLong)
	}

	/**
	 * 在cache中追加新的区域属性map并设值<br>
	 */
	private def addCacheAreaStayTime(labelsPropMap: Map[String, Map[String, String]],
	                                 location: String,
	                                 firstTime: Long,
	                                 lastTime: Long) {
		val map = Map[String, String]()
		updateCacheStayTime(map, firstTime, lastTime)
		labelsPropMap += (location -> map)
	}

	/**
	 * 更新cache中区域属性map的firstTime,lastTime值<br>
	 */
	private def updateCacheStayTime(map: Map[String, String],
	                                firstTime: Long,
	                                lastTime: Long) {
		map += (LabelConstant.LABEL_STAY_FIRSTTIME -> firstTime.toString)
		map += (LabelConstant.LABEL_STAY_LASTTIME -> lastTime.toString)
	}


	override def attachLabel(line: mutable.Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): StreamingCache = {
		cache
	}
}


class LabelProps extends StreamingCache with Serializable {
	var labelsPropList = immutable.Map[String, immutable.Map[String, String]]()
}