package com.asiainfo.ocdp.stream.label

import com.asiainfo.ocdp.stream.common.StreamingCache
import scala.collection.mutable

/**
 * Created by tsingfu on 15/9/14.
 */
class MCExtLastimeiRule extends Label {
  override def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {

    val changeImeiCache = if (cache == null) new ChangeImeiProps else cache.asInstanceOf[ChangeImeiProps]

    val newLine = line.+("last_imei" -> changeImeiCache.cacheValue)

    //更新cache
    changeImeiCache.cacheValue = line("imei")

    (newLine, changeImeiCache)
  }

}

class ChangeImeiProps extends StreamingCache with Serializable {
  var cacheValue: String = ""
}