package com.asiainfo.ocdp.stream.label

import com.asiainfo.ocdp.stream.common.StreamingCache
import com.asiainfo.ocdp.stream.constant.LabelConstant
import org.slf4j.LoggerFactory
import scala.collection.mutable
import com.asiainfo.ocdp.stream.config.LabelConf

/**
 * Created by surq on 15/12/20.
 */
class SiteRule extends Label {

  val logger = LoggerFactory.getLogger(this.getClass)
  //区域标签前缀
  val type_sine = "area_"
  //区域信息标签前缀
  val info_sine = "areainfo_"
  override def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {
    // 初始化用户定义需求标签值""
    val lebleLine = fieldsMap()
    // 用户定义需求标签字段
    val conf_lable_items = conf.getFields
    // 根据largeCell解析出所属区域
    val cachedArea = labelQryData.get(getQryKeys(line).head).get

    // 用户定义区域信息标签集合
    val conf_lable_info_items = conf_lable_items.filter(item => if (item.startsWith(info_sine)) true else false)

    // 标记业务区域标签： 如果codis中，存在item为areas的字段，则取其相关区域
    if (cachedArea.contains(LabelConstant.LABEL_AREA_LIST_KEY)) {
      // 从codis中取区域
      val areas = cachedArea(LabelConstant.LABEL_AREA_LIST_KEY).trim()
      if (areas != null && areas != "") {
        // 信令所在区域列表
        val areasList = areas.split(",")
        // 只打信令所在区域中要指定的那些区域字段
        areasList.foreach(area => {
          // 添加区域标签前缀
          val labelKey = type_sine + area.trim
          // 在用户定义标签范围内则打标签
          if (conf_lable_items.contains(labelKey)) {
            // 打区域标签
            lebleLine.update(labelKey, "true")
          }
        })
      }
    }

    // 打区域信息标签
    conf_lable_info_items.foreach(info => {
      // 去除［areainfo_］前缀
      val prop = info.substring(9)
      lebleLine.update(info, cachedArea.getOrElse(prop, ""))
    })
          
    //    // 标记行政区域标签
    //    // 20150727 新增上下班业务标签 labels['area_info']['lac_ci']
    //    val areainfo_data = cachedArea.filter(_._1 != LabelConstant.LABEL_AREA_LIST_KEY).map(x => (info_sine + x._1 -> x._2))
    //    // 所打标签集合
    //     newLine = newLine ++ areainfo_data 
    //     // 已打标签集合
    //     val labelKey = newLine.keys.toList
    //     conf.getFields.foreach(labelField => if (!labelField.contains(labelField)) newLine += (labelField ->""))

    lebleLine ++= line
    (lebleLine.toMap, cache)
  }

  /**
   * @param line:MC信令对像
   * @return codis数据库的key
   */
  override def getQryKeys(line: Map[String, String]): Set[String] = Set[String]("lacci2area:" + line("lac") + ":" + line("ci"))

}
