package com.asiainfo.ocdp.stream.label

import com.asiainfo.ocdp.stream.common.StreamingCache
import com.asiainfo.ocdp.stream.constant.LabelConstant
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * Created by liugang on 16-3-3.
 */
class TermRule extends Label{
  val logger = LoggerFactory.getLogger(this.getClass)
  //终端标签前缀
  val type_sine = "term_"
  //终端信息(codis)前缀
  val info_sine = "terminfo_"
  override def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {

    val normal_imei = line("imei").substring(5)

    val info_cols = conf.get("term_info_cols").split(",")
    val qryKeys = getQryKeys(line)

    var fieldMap = fieldsMap()

    if (qryKeys.size == 0) {
      // do nothing
    } else if (qryKeys.size == 1) {
      //其中一个imei无效
      val qryKey = qryKeys.head
      val userKey = qryKey.split(":")(1).substring(5)
      val term_info_map = labelQryData.get(qryKey).get

      if (userKey == normal_imei) {
        //常规业务的用户标签:由term_info_cols配置，逗号分隔
        info_cols.foreach(labelName => {
          term_info_map.get(labelName) match {
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
      val term_info_map = labelQryData.getOrElse("terminfo:" + normal_imei, Map[String, String]())

      info_cols.foreach(labelName => {
        term_info_map.get(labelName) match {
          case Some(value) => fieldMap += (labelName -> value)
          case None =>
          //发现：现场环境有很多 userinfo:normal_imsi 在redis中没有cache信息，也可能是外地用户，故取消executor日志打印
          //            logger.debug("= = " * 15 +"in UserBaseInfoRule, got null from labelQryData for key field  = userinfo:" + normal_imsi +" " + labelName)
        }
      })
      qryKeys.foreach(qryKey => {
        val userKey = qryKey.split(":")(1).substring(5)
        val term_info_map = labelQryData.get(qryKey).get

        //特殊业务的用户标签
        if (userKey != normal_imei) {
          //特殊业务的用户标签:在常规业务标签上加前缀
          info_cols.foreach(labelName => {
            term_info_map.get(labelName) match {
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
    Set[String](line("callingimei"), line("calledimei")).
      filterNot(value => {
      value == null || value == "000000000000000"
    }).map("terminfo:" + _)

}
