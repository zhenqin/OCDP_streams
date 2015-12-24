package com.asiainfo.ocdp.stream.label

import com.asiainfo.ocdp.stream.common.StreamingCache
import com.asiainfo.ocdp.stream.tools.DateFormatUtils
import scala.collection.mutable
import com.asiainfo.ocdp.stream.config.LabelConf

/**
 * Created by tsingfu on 15/9/14.
 */
class LabelTrack extends Label {
  //  def attachMCLabel(mcLogRow: Row, cache: StreamingCache, labelQryData: Map[String, Map[String, String]]): StreamingCache
  override def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {

    //获取cache
    /** Codis Cache: 从Codis(用户实时标签对象表)取出: 用户上一次出现基站经纬度; 上一出现时间 */
    val labelTrackCache = if (cache == null) new LabelTrackProps else cache.asInstanceOf[LabelTrackProps]

    val geo_longitude_old = labelTrackCache.cacheTrack.get("geo_longitude") match {
      case Some(v) => v
      case None => "0"
    }
    val geo_latitude_old = labelTrackCache.cacheTrack.get("geo_latitude_old") match {
      case Some(v) => v
      case None => "0"
    }
    val time_old = labelTrackCache.cacheTrack.get("time") match {
      case Some(v) => v
      case None => "0"
    }

    /** Realtime join: 根据当前信令数据, 实时关联Codis(基站信息表), 取出当前基站经纬度 */
    val cachedArea = labelQryData.get(getQryKeys(line).head).get
    val geo_longitude_new = if (cachedArea.contains("geo_longitude")) cachedArea("geo_longitude")
    else "0"
    val geo_latitude_new = if (cachedArea.contains("geo_latitude")) cachedArea("geo_latitude")
    else "0"

    /** Calc distance: 计算用户两次出现基站之间距离 */
    val distance = getDistance(Seq[String](geo_longitude_new, geo_longitude_new, geo_latitude_old, geo_longitude_old))

    /** Contract last appear lac_id, cell_id: */
    val last_lacCi = line("lac") + line("ci")
    val timeMs = DateFormatUtils.dateStr2Ms(line("time"), "yyyyMMdd HH:mm:ss.SSS")
    val speed = distance / (timeMs - time_old.toDouble)

    /** Extended stream label: 扩展实时标签到流数据() */
    val newLine = fieldsMap()
    // modify by surq at 2015.12.20 start
    //    newLine += ("speed" -> speed.toString)
    //    newLine += ("distance" -> distance.toString)
    //    newLine += ("last_lac_ci" -> last_lacCi)
    newLine.update("speed", speed.toString)
    newLine.update("distance", distance.toString)
    newLine.update("last_lac_ci", last_lacCi)
    // modify by surq at 2015.12.20 end
    newLine ++= line

    /** Update Codis Realtime Object: 更新Codis(用户实时标签对象表) */
    labelTrackCache.cacheTrack = Map[String, String](
      "geo_longitude" -> geo_longitude_new, "geo_latitude" -> geo_latitude_new, "time" -> timeMs.toString)

    (newLine.toMap, labelTrackCache)
  }

  /**
   * @param line:MC信令对像
   * @return codis数据库的key
   */
  override def getQryKeys(line: Map[String, String]): Set[String] = Set[String]("lacci2area:" + line("lac") + ":" + line("ci"))

  def rad(d: Double): Double = {
    d * Math.PI / 180.0
  }

  /**
   * @constructor  计算经纬度距离函数
   * @return 经纬度距离函数
   */
  def getDistance(seq: Seq[String]): Double = {
    val EARTH_RADIUS = 6378.137

    val paraList = seq.map(_.trim.toDouble)
    val lat1 = paraList(0)
    val lng1 = paraList(1)
    val lat2 = paraList(2)
    val lng2 = paraList(3)

    val radLat1 = rad(lat1)
    val radLat2 = rad(lat2)
    val a = radLat1 - radLat2
    val b = rad(lng1) - rad(lng2)

    var s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) +
      Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)))
    s = s * EARTH_RADIUS
    Math.round(s * 10000) / 10000
  }

}

class LabelTrackProps extends StreamingCache with Serializable {
  var cacheTrack = Map[String, String]()
}