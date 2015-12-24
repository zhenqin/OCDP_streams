package com.asiainfo.ocdp.stream.label

import com.asiainfo.ocdp.stream.common.StreamingCache
import scala.collection.mutable
import com.asiainfo.ocdp.stream.config.LabelConf

/**
 * Created by tsingfu on 15/9/14.
 */
class ExtLastimeiRule extends Label {
  override def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {

    val changeImeiCache = if (cache == null) new ChangeImeiProps else cache.asInstanceOf[ChangeImeiProps]

    val newLine = fieldsMap()
    newLine.update("last_imei", changeImeiCache.cacheValue)
    newLine ++= line

    //更新cache
    changeImeiCache.cacheValue = line("imei")

    (newLine.toMap, changeImeiCache)
  }

}

class ChangeImeiProps extends StreamingCache with Serializable {
  var cacheValue: String = ""
}