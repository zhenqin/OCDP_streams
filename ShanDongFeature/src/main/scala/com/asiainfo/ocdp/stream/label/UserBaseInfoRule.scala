package com.asiainfo.ocdp.stream.label

import com.asiainfo.ocdp.stream.common.StreamingCache
import org.slf4j.LoggerFactory
import scala.collection.mutable
import com.asiainfo.ocdp.stream.config.LabelConf

/**
 * Created by leo on 4/29/15.
 */
class UserBaseInfoRule extends Label {

  val logger = LoggerFactory.getLogger(this.getClass)

  override def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {

    val normal_imsi = line("imsi")

    val info_cols = conf.get("user_info_cols").split(",")
    val qryKeys = getQryKeys(line)

    var fieldMap = fieldsMap()

    if (qryKeys.size == 0) {
      // do nothing
    } else if (qryKeys.size == 1) {
      //其中一个imsi无效
      val qryKey = qryKeys.head
      val userKey = qryKey.split(":")(1)
      val user_info_map = labelQryData.get(qryKey).get

      if (userKey == normal_imsi) {
        //常规业务的用户标签:由user_info_cols配置，逗号分隔
        info_cols.foreach(labelName => {
          user_info_map.get(labelName) match {
            case Some(value) =>
              fieldMap += (labelName -> value)
            case None =>
          }
        })
      } else {
        // do nothing
      }
    } else if (qryKeys.size == 2) {

      //常规业务用户标签
      val user_info_map = labelQryData.getOrElse("userinfo:" + normal_imsi, Map[String, String]())

      info_cols.foreach(labelName => {
        user_info_map.get(labelName) match {
          case Some(value) => fieldMap += (labelName -> value)
          case None =>
          //发现：现场环境有很多 userinfo:normal_imsi 在redis中没有cache信息，也可能是外地用户，故取消executor日志打印
          //            logger.debug("= = " * 15 +"in UserBaseInfoRule, got null from labelQryData for key field  = userinfo:" + normal_imsi +" " + labelName)
        }
      })
      qryKeys.foreach(qryKey => {
        val userKey = qryKey.split(":")(1)
        val user_info_map = labelQryData.get(qryKey).get

        //特殊业务的用户标签
        if (userKey != normal_imsi) {
          //特殊业务的用户标签:在常规业务标签上加前缀
          info_cols.foreach(labelName => {
            user_info_map.get(labelName) match {
              case Some(value) =>
                fieldMap += (if (userKey == line("calledimsi")) ("called_" + labelName -> value) else ("calling_" + labelName -> value))
              case None =>
            }
          })
        } else {
          // do nothing
        }
      })
    } else {
      // do nothing
    }
     
    //    line.foreach(fieldMap.+(_))
    fieldMap ++= line

    (fieldMap.toMap, cache)
  }

  override def getQryKeys(line: Map[String, String]): Set[String] =
    Set[String](line("callingimsi"), line("calledimsi")).
      filterNot(value => {
      value == null || value == "000000000000000"
    }).map("userinfo:" + _)


}
