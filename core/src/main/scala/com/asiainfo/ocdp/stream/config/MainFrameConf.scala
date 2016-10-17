package com.asiainfo.ocdp.stream.config

import com.asiainfo.ocdp.stream.common.JDBCUtil
import com.asiainfo.ocdp.stream.constant.TableInfoConstant
import com.asiainfo.ocdp.stream.tools.Json4sUtils
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * Created by leo on 8/12/15.
 */

object MainFrameConf {

  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName.replace("$", ""))

  val systemProps = new SystemProps()

  val dsid2DataSourceConfMap = mutable.Map[String, DataSourceConf]()
  val diid2DataInterfaceConfMap = mutable.Map[String, DataInterfaceConf]()

  val diid2LabelId2LabelRuleConfMap = mutable.Map[String, mutable.Map[String, LabelConf]]()
  val diid2EventId2EventRuleConfMap = mutable.Map[String, mutable.Map[String, EventConf]]()
  val beid2EventIdsMap = mutable.Map[String, mutable.ArrayBuffer[String]]()
  val eventId2CacheKeyNameMap = mutable.Map[String, String]()

  val diid2SchemaMap = mutable.Map[String, StructType]()
  val diid2PropsMap = mutable.Map[String, Map[String, String]]()
  val eventId2DIidMap = mutable.Map[String, String]()
  val diid2EventId2ParentEventId = mutable.Map[String, mutable.Map[String, String]]()

  val beid2OutputDIid2DIConfsMap = mutable.Map[String, mutable.Map[String, DataInterfaceConf]]()
  val beid2InputDIid2DIConfsMap = mutable.Map[String, mutable.Map[String, DataInterfaceConf]]()
  val beid2EventId2SelectExprMap = mutable.Map[String, mutable.Map[String, String]]()
  val beid2EventIdSelectExprArrMap = mutable.Map[String, mutable.ArrayBuffer[(String, String)]]()


  val beid2OutputDIidsMap = mutable.Map[String, mutable.Set[String]]()
  val beid2InputDIidsMap = mutable.Map[String, mutable.Set[String]]()

  val diid2BeidsMap = mutable.Map[String, mutable.Set[String]]()
  val beid2BeconfMap = mutable.Map[String, SubjectConf]()

  initMainFrameConf()

  def initMainFrameConf(): Unit = {
    initSystemProps()
    /*initDataSourceConf()
    initDataInterfaceConf()
    initLabelRuleConf()
    initEventRuleConf()
    initBusinessEventConf()*/
    println("= = " * 20 +" finish initMainFrameConf")
  }

  /**
   * init SystemProps config
   */
  def initSystemProps() {
    val sql = "select name,value from " + TableInfoConstant.SystemPropTableName
    val sysdata = JDBCUtil.query(sql)
    sysdata.foreach(x => {
      systemProps.set(x.get("name").get, x.get("value").get)
    })
  }
}
