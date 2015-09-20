package com.asiainfo.ocdp.stream.service

import com.asiainfo.ocdp.stream.common.{JDBCUtil, Logging}
import com.asiainfo.ocdp.stream.config._
import com.asiainfo.ocdp.stream.constant.TableInfoConstant
import com.asiainfo.ocdp.stream.event.Event
import com.asiainfo.ocdp.stream.label.Label
import com.asiainfo.ocdp.stream.tools.Json4sUtils
import scala.collection.mutable._

/**
 * Created by leo on 9/16/15.
 */
class DataInterfaceServer extends Logging {

  def getDataInterfaceInfoById(id: String): DataInterfaceConf = {

    val conf = new DataInterfaceConf()

    val sql = "select id, name, dsid, type, status, delim, properties " +
      "from " + TableInfoConstant.DataInterfaceTableName +
      " where id='" + id + "' and status = 1"

    val interface = JDBCUtil.query(sql).head
    conf.setDiid(interface.get("id").get)
    conf.setName(interface.get("name").get)
    conf.setDiType(interface.get("type").get.toInt)

    val dsconf = getDataSourceInfoById(interface.get("dsid").get)
    conf.setDsConf(dsconf)

    val propsJsonStr = interface.get("properties").getOrElse(null)
    conf.setBaseSchema(Json4sUtils.jsonStr2StructType(propsJsonStr, "fields"))
    conf.setUDFSchema(Json4sUtils.jsonStr2StructType(propsJsonStr, "UDFfields"))

    val propsMap = Json4sUtils.jsonStr2ArrMap(propsJsonStr, "props")
    propsMap.foreach(kvMap => {
      if (!kvMap.isEmpty) conf.set(kvMap.get("pname").get, kvMap.get("pvalue").get)
    })
    conf
  }

  def getDataSourceInfoById(id: String): DataSourceConf = {
    val conf = new DataSourceConf()
    val sql = "select name,type,properties from " + TableInfoConstant.DataSourceTableName + " where id = '" + id + "'"

    val datasource = JDBCUtil.query(sql).head

    conf.setDsid(datasource.get("dsid").get)
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
    val sql = "select id, plabelid, name, class_name, properties " +
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
      conf.setPlabelId(x.get("plabelid").get)

      val propsJsonStr = x.get("properties").getOrElse(null)
      if (propsJsonStr != null) {
        val propsArrMap = Json4sUtils.jsonStr2ArrMap(propsJsonStr, "props")
        propsArrMap.foreach(kvMap => {
          if (!kvMap.isEmpty) conf.set(kvMap.get("pname").get, kvMap.get("pvalue").get)
        })
      }

      val label: Label = Class.forName(conf.getClass_name).newInstance().asInstanceOf[Label]
      label.init(conf)
      labelarr += label
    })

    labelarr.sortWith((l1, l2) => {
      val i1 = l1.conf.getId
      val p1 = l1.conf.getPlabelId
      val i2 = l2.conf.getId
      val p2 = l2.conf.getPlabelId
      p1.equals(i2) || p2.equals(i1) || i1.compareTo(i2) < 0
    }).toArray
  }

  def getEventsByIFId(id: String): Array[Event] = {
    val sql = "select id, name, event_expr, peventid, properties " +
      "from " + TableInfoConstant.EventTableName +
      " where diid = '" + id + "' status = 1"
    val data = JDBCUtil.query(sql)

    val eventarr = ArrayBuffer[Event]()
    data.foreach(x => {
      val conf = new EventConf()
      conf.setId(x.get("id").get)
      conf.setInIFId(id)
      conf.setName(x.get("name").get)
      conf.setEvent_expr(x.get("event_expr").get)
      conf.setP_event_id(x.get("peventid").get)

      val propsJsonStr = x.get("properties").getOrElse(null)

      // 业务对应的输出数据接口配置，每个业务一个输出事件接口
      val outputIFIdsArrMap = Json4sUtils.jsonStr2ArrMap(propsJsonStr, "output_dis")
      val outputIFIdArr = ArrayBuffer[DataInterfaceConf]()
      outputIFIdsArrMap.foreach(kvMap => {
        val ifid = kvMap.get("pvalue").get
        outputIFIdArr += (getDataInterfaceInfoById(ifid))
      })
      conf.setOutIFIds(outputIFIdArr.toArray)

      val event = new Event
      event.init(conf)
      eventarr += event
    })

    eventarr.sortWith((e1, e2) => {
      val i1 = e1.conf.id
      val p1 = e1.conf.p_event_id
      val i2 = e2.conf.id
      val p2 = e2.conf.p_event_id
      p1.equals(i2) || p2.equals(i1) || i1.compareTo(i2) < 0
    }).toArray
  }

}
