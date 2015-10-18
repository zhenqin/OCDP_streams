package com.asiainfo.ocdp.stream.label

import com.asiainfo.ocdp.stream.common.StreamingCache
import com.asiainfo.ocdp.stream.constant.LabelConstant
import org.slf4j.LoggerFactory

import scala.collection.mutable


/**
 * Created by tsingfu on 15/8/26.
 */
class SiteRule extends Label {

  val logger = LoggerFactory.getLogger(this.getClass)

  val type_sine = "area_"

  override def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {
    // 装载业务区域标签属性
    var newLine = fieldsMap()
    // 根据largeCell解析出所属区域
    //    println("current lacci : " + getQryKeys(logRow))
    val cachedArea = labelQryData.get(getQryKeys(line).head).get
    logger.debug("= = " * 20 + " cachedArea = " + cachedArea.mkString("[", ",", "]"))

    // 标记业务区域标签
    if (cachedArea.contains(LabelConstant.LABEL_AREA_LIST_KEY)) {
      val areas = cachedArea(LabelConstant.LABEL_AREA_LIST_KEY).trim()
      if (areas != "") areas.split(",").foreach(area => {
        //        println("current area : " + area)
        newLine += (type_sine + area.trim -> "true")
      })
    }

    logger.debug("= = " * 20 + " onSiteMap = " + newLine.mkString("[", ",", "]"))

    // 标记行政区域标签
    // 20150727 新增上下班业务标签 labels['area_info']['lac_ci']
    newLine = newLine ++ cachedArea.filter(_._1 != LabelConstant.LABEL_AREA_LIST_KEY) ++ Map("lac_ci" -> (line("lac") + line("ci"))) ++ line

    (newLine.toMap, cache)
  }

  /**
   * @param line:MC信令对像
   * @return codis数据库的key
   */
  override def getQryKeys(line: Map[String, String]): Set[String] = Set[String]("lacci2area:" + line("lac") + ":" + line("ci"))


}
