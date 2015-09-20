package com.asiainfo.ocdp.stream.service

import com.asiainfo.ocdp.stream.common.JDBCUtil
import com.asiainfo.ocdp.stream.config.SubjectConf
import com.asiainfo.ocdp.stream.constant.TableInfoConstant
import com.asiainfo.ocdp.stream.tools.Json4sUtils

/**
 * Created by leo on 9/21/15.
 */
class SubjectServer {

  def getSubjectInfoById(id: String): SubjectConf = {
    val conf = new SubjectConf()

    val sql = "select name, properties " +
      "from " + TableInfoConstant.BusinessEventTableName +
      " where id='" + id + "' and status = 1"

    val subject = JDBCUtil.query(sql).head
    conf.setId(id)
    conf.setName(subject.get("name").get)

    val propsJsonStr = subject.get("properties").getOrElse(null)
    var propsMap = Json4sUtils.jsonStr2ArrMap(propsJsonStr, "events")

    propsMap.foreach(kvMap => {
      if (!kvMap.isEmpty) {
        conf.set(kvMap.get("eventId").get, kvMap.get("select_expr").get)
      }
    })

    /*var propsMap = Json4sUtils.jsonStr2ArrMap(propsJsonStr, "props")
    propsMap.foreach(kvMap => {
      if (!kvMap.isEmpty) conf.set(kvMap.get("pname").get, kvMap.get("pvalue").get)
    })*/

    conf
  }
}
