package com.asiainfo.ocdp.streaming.label

import com.asiainfo.ocdp.streaming.common.StreamingCache
import com.asiainfo.ocdp.streaming.constant.LabelConstant
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types.StructType

import scala.collection.mutable
import scala.collection.mutable.Map

/**
 * Created by tsingfu on 15/9/14.
 */
class MCExtLastimei extends MCLabel {
	//  def attachMCLabel(mcLogRow: Row, cache: StreamingCache, labelQryData: Map[String, Map[String, String]]): StreamingCache
	override def attachMCLabel(mcLogRow: GenericMutableRow, schema: StructType, cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): StreamingCache = {

		//更新cache
		val changeImeiCache = if(cache == null) new ChangeImeiProps else cache.asInstanceOf[ChangeImeiProps]
		val numFields = mcLogRow.length
		val lablesMap = mcLogRow.getAs[String](numFields - 1).asInstanceOf[Map[String, Map[String, String]]]

		changeImeiCache.cacheValue = lablesMap.get("FirstLabel").get.get("imei").get

		//更新标签
		val mMap = Map[String, String]("imei" -> changeImeiCache.cacheValue)
		lablesMap.put(LabelConstant.LABEL_LASTIMEI, mMap)


		changeImeiCache
	}

	override def attachLabel(line: mutable.Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): StreamingCache = {
		cache
	}
}

class ChangeImeiProps extends StreamingCache with Serializable {
	var cacheValue: String = ""
}