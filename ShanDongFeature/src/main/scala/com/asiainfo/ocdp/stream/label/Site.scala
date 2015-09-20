package streaming.label

import com.asiainfo.ocdp.streaming.common.StreamingCache
import com.asiainfo.ocdp.streaming.constant.LabelConstant
import com.asiainfo.ocdp.streaming.datasource.DataInterface
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.collection.mutable


/**
 * Created by tsingfu on 15/8/26.
 */
class Site extends MCLabel {

  val logger = LoggerFactory.getLogger(this.getClass)

  override def attachMCLabel(mcLogRow: GenericMutableRow, schema: StructType, cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): StreamingCache = {
    // 装载业务区域标签属性
    val onSiteMap = scala.collection.mutable.Map[String, String]()
    // 根据largeCell解析出所属区域
    //    println("current lacci : " + getQryKeys(logRow))
    val cachedArea = labelQryData.get(getQryKeys(mcLogRow, schema).head).get
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
    val labelsMap = mcLogRow.getAs[mutable.Map[String, mutable.Map[String, String]]](DataInterface.getFieldIdx(schema, LabelConstant.LABEL_MAP_KEY))
    labelsMap.put(LabelConstant.LABEL_ONSITE, onSiteMap)
    // 标记行政区域标签
    // 20150727 新增上下班业务标签 labels['area_info']['lac_ci']
    val lac_ci = mcLogRow.getAs[String](DataInterface.getFieldIdx(schema, "lac")) + mcLogRow.getAs[String](DataInterface.getFieldIdx(schema, "ci"))
    labelsMap.put(LabelConstant.LABEL_AREA, cachedArea.filter(_._1 != LabelConstant.LABEL_AREA_LIST_KEY)
            ++ Map("lac_ci" -> lac_ci))

    cache
  }

  /**
   * @param mcLogRow:MC信令对像
   * @return codis数据库的key
   */
  override def getQryKeys(mcLogRow: GenericMutableRow, schema: StructType): Set[String] = {
//    val mcsource = mcLogRow.asInstanceOf[Row]
//    Set[String]("lacci2area:" + mcLogRow.getAs[String]("lac") + ":" + mcLogRow.getAs[String]("ci"))
	  Set[String]("lacci2area:" + mcLogRow.getAs[String](DataInterface.getFieldIdx(schema, "lac")) + ":" + mcLogRow.getAs[String](DataInterface.getFieldIdx(schema, "ci")))
  }

	override def attachLabel(line: mutable.Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): StreamingCache = {
		cache
	}
}
