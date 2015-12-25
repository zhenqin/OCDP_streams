package com.asiainfo.ocdp.stream.service

import com.asiainfo.ocdp.stream.common.{ JDBCUtil, Logging }
import com.asiainfo.ocdp.stream.config._
import com.asiainfo.ocdp.stream.constant.{ DataSourceConstant, TableInfoConstant }
import com.asiainfo.ocdp.stream.event.Event
import com.asiainfo.ocdp.stream.label.Label
import com.asiainfo.ocdp.stream.tools.Json4sUtils
import scala.collection.mutable._

/**
 * Created by leo on 9/16/15.
 */
class DataInterfaceServer extends Logging with Serializable {

  def getDataInterfaceInfoById(id: String): DataInterfaceConf = {

    val conf = new DataInterfaceConf()
    // modify by surq at 2015.11.09 start
    //    val sql = "select id, name, dsid, type, status, properties " +
    val sql = "select id, filter_expr,name, dsid, type, status, properties " +
      // modify by surq at 2015.11.09 end
      "from " + TableInfoConstant.DataInterfaceTableName +
      " where id='" + id + "' and status = 1"

    val result = JDBCUtil.query(sql)

    if (result.length > 0) {
      val interface = result.head
      conf.setDiid(interface.get("id").get)
      conf.setName(interface.get("name").get)
      conf.setDiType(interface.get("type").get.toInt)
      // add by surq at 2015.11.09 start
      conf.set("filter_expr", interface.get("filter_expr").get)
      // add by surq at 2015.11.09 end
      val dsconf = getDataSourceInfoById(interface.get("dsid").get)
      conf.setDsConf(dsconf)

      val propsJsonStr = interface.get("properties").getOrElse(null)
      conf.setBaseSchema(Json4sUtils.jsonStr2BaseStructType(propsJsonStr, "fields"))
      conf.setBaseItemsSize((Json4sUtils.jsonStr2ArrMap(propsJsonStr, "fields")).size)
      conf.setAllItemsSchema(Json4sUtils.jsonStr2UdfStructType(propsJsonStr, "fields", "userFields"))

      val propsMap = Json4sUtils.jsonStr2ArrMap(propsJsonStr, "props")
      propsMap.foreach(kvMap => {
        if (!kvMap.isEmpty) conf.set(kvMap.get("pname").get, kvMap.get("pvalue").get)
      })
    }
    conf
  }

  def getDataSourceInfoById(id: String): DataSourceConf = {
    val conf = new DataSourceConf()
    val sql = "select name,type,properties from " + TableInfoConstant.DataSourceTableName + " where id = '" + id + "'"

    val datasource = JDBCUtil.query(sql).head

    conf.setDsid(id)
    conf.setName(datasource.get("name").get)
    conf.setDsType(datasource.get("type").get)

    val propsJsonStr = datasource.get("properties").getOrElse(null)
    val propsArrMap = Json4sUtils.jsonStr2ArrMap(propsJsonStr)
    propsArrMap.foreach { kvMap =>
      if (!kvMap.isEmpty) conf.set(kvMap.get("pname").get, kvMap.get("pvalue").get)
    }
    conf
  }

  def getLabelsByIFId(id: String): Array[Label] = {
    val sql = "select id, p_label_id, name, class_name, properties " +
      "from " + TableInfoConstant.LabelTableName +
      " where diid = '" + id + "' and status = 1"
    val dsdata = JDBCUtil.query(sql)

    val labelarr = ArrayBuffer[Label]()
    dsdata.foreach(x => {
      val conf = new LabelConf()
      conf.setId(x.get("id").get)
      conf.setDiid(id)
      conf.setName(x.get("name").get)
      conf.setClass_name(x.get("class_name").get)
      conf.setPlabelId(x.get("p_label_id").get)

      val propsJsonStr = x.get("properties").getOrElse(null)
      if (propsJsonStr != null) {
        val propsArrMap = Json4sUtils.jsonStr2ArrMap(propsJsonStr, "props")
        propsArrMap.foreach(kvMap => {
          if (!kvMap.isEmpty) conf.set(kvMap.get("pname").get, kvMap.get("pvalue").get)
        })

        val fieldsArrMap = Json4sUtils.jsonStr2ArrMap(propsJsonStr, "labelItems")
        val fieldsList = ArrayBuffer[String]()
        fieldsArrMap.foreach(kvMap => {
          if (!kvMap.isEmpty) fieldsList += kvMap.get("pvalue").get
        })
        //打标签字段
        conf.setFields(fieldsList.toList)
      }

      val label: Label = Class.forName(conf.getClass_name).newInstance().asInstanceOf[Label]
      label.init(conf)
      labelarr += label
    })

    //根据标签的依赖关系排序
    val labelIDMap = labelarr.map(label => (label.conf.id, label)).toMap
    val resultArray = ArrayBuffer[String]()
    labelarr.foreach(label => labelSort(label.conf.id, resultArray, labelIDMap))
    val result = ArrayBuffer[Label]()
    resultArray.foreach(id => {
      result += labelIDMap(id)
    })
    result.toArray

//    labelarr.sortWith((l1, l2) => {
//      val i1 = l1.conf.getId
//      val p1 = l1.conf.getPlabelId
//      val i2 = l2.conf.getId
//      val p2 = l2.conf.getPlabelId
//      p1.equals(i2) || p2.equals(i1) || i1.compareTo(i2) < 0
//    }).toArray
  }

  /**
   * 根据标签的信赖关系排序
   */
  def labelSort(labelId: String, result: ArrayBuffer[String], map: scala.collection.immutable.Map[String,Label]) {
    val pid = map(labelId).conf.plabelId
    if (pid != null && pid.trim != "") labelSort(pid, result, map)
    if (!result.contains(labelId)) result += labelId
  }
  
  def getEventsByIFId(id: String): Array[Event] = {
    /*val sql = "select id, name, select_expr, filter_expr, p_event_id, properties " +
      "from " + TableInfoConstant.EventTableName +
      " where diid = '" + id + "' and status = 1"*/
    val sql = "select id, name, select_expr, filter_expr, p_event_id, properties " +
      "from " + TableInfoConstant.EventTableName +
      " where diid = '" + id + "' and status = 1"
    val data = JDBCUtil.query(sql)

    val eventarr = ArrayBuffer[Event]()
    data.foreach(x => {
      val conf = new EventConf()
      conf.setId(x.get("id").get)
      conf.setInIFId(id)
      conf.setName(x.get("name").get)
      conf.setSelect_expr(x.get("select_expr").get)
      conf.setFilte_expr(x.get("filter_expr").get)
      conf.setP_event_id(x.get("p_event_id").get)

      val propsJsonStr = x.get("properties").getOrElse(null)

      //
      val propsMap = Json4sUtils.jsonStr2ArrMap(propsJsonStr, "props")
      propsMap.foreach(kvMap => conf.set(kvMap.get("pname").get, kvMap.get("pvalue").get))

      // 业务对应的输出数据接口配置，每个业务一个输出事件接口
      // surq: 存放输出信息：[interfaceID->,delim->,interval->]
      val outputIFIdsArrMap = Json4sUtils.jsonStr2ArrMap(propsJsonStr, "output_dis")
      val outputIFIdArr = ArrayBuffer[DataInterfaceConf]()

      //TODO send_interval should bound on dataInterface, now just one di , so set the last value
      var send_interval = 0
      var delim = ","
      outputIFIdsArrMap.foreach(kvMap => {
        val diid = kvMap.get("diid").get
        // 加载Interface内容
        outputIFIdArr += (getDataInterfaceInfoById(diid))
        send_interval = kvMap.get("interval").get.toInt
        delim = kvMap.get("delim").get
      })
      conf.setOutIFIds(outputIFIdArr.toArray)
      conf.setInterval(send_interval)
      conf.setDelim(delim)
      val event = new Event
      event.init(conf)
      eventarr += event
    })
    eventarr.sortWith((e1, e2) => {
      val i1 = e1.conf.id
      val p1 = e1.conf.p_event_id
      val i2 = e2.conf.id
      val p2 = e2.conf.p_event_id
      i2.equals(p1) || i1.equals(p2) || i1.compareTo(i2) < 0
    }).toArray
  }

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

    val events = Map[String, String]()
    propsMap.foreach(kvMap => {
      if (!kvMap.isEmpty) {
        events += (kvMap.get("eventId").get -> kvMap.get("select_expr").get)
      }
    })
    conf.setEvents(events.toMap)

    propsMap = Json4sUtils.jsonStr2ArrMap(propsJsonStr, "props")
    propsMap.foreach(kvMap => {
      if (!kvMap.isEmpty) conf.set(kvMap.get("pname").get, kvMap.get("pvalue").get)
    })

    conf
  }

  def getDataInterfaceByEventId(ids: Array[String]): Array[DataInterfaceConf] = {

    val sql = "select properties " +
      "from " + TableInfoConstant.EventTableName +
      " where id in (" + ids.map("'" + _ + "'").mkString(",") + ") and status = 1"
    val data = JDBCUtil.query(sql)

    val dfarr = new ArrayBuffer[DataInterfaceConf]()
    data.map(x => {
      val propsJsonStr = x.get("properties").getOrElse(null)
      val outputIFIdsArrMap = Json4sUtils.jsonStr2ArrMap(propsJsonStr, "output_dis")
      outputIFIdsArrMap.foreach(kvMap => {
        val ifid = kvMap.get("pvalue").get
        val conf = getDataInterfaceInfoById(ifid)
        if (DataSourceConstant.KAFKA_TYPE.equals(conf.getDiType))
          dfarr.append(conf)
      })
    })

    dfarr.toArray
  }
}
