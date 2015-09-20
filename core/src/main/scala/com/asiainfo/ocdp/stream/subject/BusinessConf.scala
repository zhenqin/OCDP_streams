package com.asiainfo.ocdp.stream.subject

import com.asiainfo.ocdp.stream.common.JDBCUtil
import com.asiainfo.ocdp.stream.config.{DataInterfaceConf, DataSourceConf, BaseConf}
import com.asiainfo.ocdp.stream.constant.TableInfoConstant
import com.asiainfo.ocdp.stream.tools.Json4sUtils

import scala.collection.mutable

/**
 * Created by tsingfu on 15/8/20.
 */
class BusinessConf extends BaseConf {

}

object BusinessConf {

  // 数据源配置
  val dataSourceConfMaps = mutable.Map[String, DataSourceConf]()

  // 数据接口配置
  val dataInterfaceConfMap = mutable.Map[String, DataInterfaceConf]()

  /**
   * init dataSource config
   */
  def initDataSourceConf() {
    val sql = "select ds.id as dsid, ds.name, ds.type, ds.props " +
            "from " + TableInfoConstant.DataSourceTableName + " ds " +
            "left join " + TableInfoConstant.DataInterfaceTableName + " di " +
            "on ds.id = di.dsid where di.status = 1"
    val dsdata = JDBCUtil.query(sql)

    dsdata.foreach(x=>{
      val dataSourceConf = new DataSourceConf()

      dataSourceConf.set("dsid", x.get("id").get)
      dataSourceConf.set("name", x.get("name").get)
      dataSourceConf.set("type", x.get("type").get)

      val propsJsonStr = x.get("props").getOrElse(null)
      val propsMap = Json4sUtils.jsonStr2Map(propsJsonStr)
      propsMap.foreach(kv => {
        dataSourceConf.set(kv._1, kv._2)
      })

      dataSourceConfMaps.put(dataSourceConf.get("dsid"), dataSourceConf)
    })
  }


  /**
   * init dataInterface config
   */
  def initDataInterfaceConf() {
    val sql = "select id, name, dsid, status, delim, filterClass, batchDurationS, props " +
            "from " + TableInfoConstant.DataInterfaceTableName +
            " where status = 1"
    val dsdata = JDBCUtil.query(sql)

    dsdata.foreach(x=>{
      val dataInterfaceConf = new DataInterfaceConf()
      dataInterfaceConf.set("diid", x.get("id").get)
      dataInterfaceConf.set("name", x.get("name").get)
      dataInterfaceConf.set("dsid", x.get("dsid").get)

      val propsJsonStr = x.get("props").orNull
      val propsMap = Json4sUtils.jsonStr2Map(propsJsonStr)
      propsMap.foreach(kv => {
        dataInterfaceConf.set(kv._1, kv._2)
      })
      dataInterfaceConfMap.put(dataInterfaceConf.get("diid"), dataInterfaceConf)
    })
  }


  /**
   * init business config
   */
//  def initBusinessConf(): Unit = {
//    val sql = ""
//    val
//  }



}