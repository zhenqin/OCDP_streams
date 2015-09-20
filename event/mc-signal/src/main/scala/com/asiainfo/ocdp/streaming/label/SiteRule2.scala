package com.asiainfo.ocdp.streaming.label

/*
import com.asiainfo.ocdp.streaming.common.StreamingCache
import com.asiainfo.ocdp.streaming.constant.LabelConstant
import com.asiainfo.ocdp.streaming.datasource.DataInterface
import org.slf4j.LoggerFactory
import scala.collection.mutable.Map

/**
 * Created by leo on 9/14/15.
 */
class SiteRule2 extends LabelRule {
  val logger = LoggerFactory.getLogger(this.getClass)

  override def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: Map[String, Map[String, String]]): StreamingCache = {
    val labelsMap = Map[String, String]

    // 装载业务区域标签属性
    val onSiteMap = Map[String, String]()
    // 根据largeCell解析出所属区域
    //    println("current lacci : " + getQryKeys(logRow))
    val cachedArea = labelQryData.get(getQryKeys(line).head).get
    logger.debug("= = " * 20 + " cachedArea = " + cachedArea.mkString("[", ",", "]"))
    if (cachedArea.contains(LabelConstant.LABEL_AREA_LIST_KEY)) {
      val areas = cachedArea(LabelConstant.LABEL_AREA_LIST_KEY).trim()
      if (areas != "") areas.split(",").foreach(area => {
        //        println("current area : " + area)
        onSiteMap += (area.trim -> "true")
      })
    }

    logger.debug("= = " * 20 + " onSiteMap = " + onSiteMap.mkString("[", ",", "]"))

    // 标记业务区域标签

    labelsMap.put(LabelConstant.LABEL_ONSITE, onSiteMap)
    // 标记行政区域标签
    // 20150727 新增上下班业务标签 labels['area_info']['lac_ci']
    val lac_ci = mcLogRow.getAs[String](DataInterface.getFieldIdx(schema, "lac")) + mcLogRow.getAs[String](DataInterface.getFieldIdx(schema, "ci"))
    labelsMap.put(LabelConstant.LABEL_AREA, cachedArea.filter(_._1 != LabelConstant.LABEL_AREA_LIST_KEY)
      ++ Map("lac_ci" -> lac_ci))

    cache
  }

  /**
   * @param line:MC信令对像
   * @return codis数据库的key
   */
  override def getQryKeys(line: Map[String, String]): Set[String] = {
    Set[String]("lacci2area:" + line.getOrElse("lac", "") + ":" + line.getOrElse("ci", ""))
  }
}
*/
