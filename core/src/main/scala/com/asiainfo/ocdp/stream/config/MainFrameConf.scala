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
// deteled by surq at 2015.11.21 start
//  /**
//   * init dataSource config
//   */
//  def initDataSourceConf() {
//    val sql = "select ds.id as dsid, ds.name, ds.type, ds.properties " +
//            "from " + TableInfoConstant.DataSourceTableName + " ds " +
//            "left join " + TableInfoConstant.DataInterfaceTableName + " di " +
//            "on ds.id = di.dsid " +
//            "where di.status = 1"
//    val dsdata = JDBCUtil.query(sql)
//
//    dsdata.foreach(x=>{
//      val dataSourceConf = new DataSourceConf()
//
//      dataSourceConf.set("dsid", x.get("dsid").get)
//      dataSourceConf.set("name", x.get("name").get)
//      dataSourceConf.set("type", x.get("type").get)
//
//      val propsJsonStr = x.get("properties").getOrElse(null)
//      val propsArrMap = Json4sUtils.jsonStr2ArrMap(propsJsonStr)
//      propsArrMap.foreach{kvMap =>
//        if (!kvMap.isEmpty) dataSourceConf.set(kvMap.get("pname").get, kvMap.get("pvalue").get)
//      }
//      dsid2DataSourceConfMap.put(dataSourceConf.get("dsid"), dataSourceConf)
//    })
//  }
//
//
//  /**
//   * init dataInterface config
//   */
//  def initDataInterfaceConf() {
//    /*val sql = "select id, name, dsid, type, status, properties, status " +
//            "from " + TableInfoConstant.DataInterfaceTableName +
//            " where status = 1"
//    val dsdata = JDBCUtil.query(sql)
//
//    dsdata.foreach(x=>{
//      val dataInterfaceConf = new DataInterfaceConf()
//      val diid = x.get("id").get
//      dataInterfaceConf.set("diid", diid)
//      dataInterfaceConf.set("name", x.get("name").get)
//      dataInterfaceConf.set("dsid", x.get("dsid").get)
//      val diType = x.get("type").get
//      dataInterfaceConf.set("type", diType)
//
//      //propsJsonStr格式：
//      // JObject{List[Jobject{fieldName:, fieldDataType:}, ...]}
//      // 格式1
//      //    """
//      //                          {"fields": [
//      //                            {"fieldName":"col1", "fieldDataType": "String"},
//      //                            {"fieldName":"col2", "fieldDataType":"Int"},
//      //                            {"fieldName":"col3", "fieldDataType":"Long"},
//      //                            {"fieldName":"col4", "fieldDataType":"Boolean"},
//      //                            {"fieldName":"col5", "fieldDataType":"Double"}
//      //                            ],
//      //                           "props": [
//      //                            {"uks" : "col1,col2", "ukSeparator" : "#"}
//      //                            ]
//      //                          }
//      //    """
//      val propsJsonStr = x.get("properties").getOrElse(null)
//
//      if (diType == "0") diid2SchemaMap.put(diid, Json4sUtils.jsonStr2StructType(propsJsonStr, "fields"))
////      val propsMap = Json4sUtils.jsonStr2ArrTuple2(propsJsonStr, Array("uks", "ukSeparator"))
//      val propsMap = Json4sUtils.jsonStr2ArrMap(propsJsonStr, "props")
//      propsMap.foreach(kvMap => {
//        if (!kvMap.isEmpty) dataInterfaceConf.set(kvMap.get("pname").get, kvMap.get("pvalue").get)
//      })
//
//      //设置使用自定义增强类
//      val className = dataInterfaceConf.get("class_name", "com.asiainfo.ocdp.stream.datasource.DataInterface")
//      dataInterfaceConf.set("class_name", className)
//
//      diid2DataInterfaceConfMap.put(dataInterfaceConf.get("diid"), dataInterfaceConf)
//
//    })*/
//  }
//
//
//  /**
//   * init labelRule config
//   */
//  def initLabelRuleConf() {
//    val sql = "select id, name, class_name, diid, status, properties " +
//            "from " + TableInfoConstant.LabelTableName +
//            " where status = 1"
//    val dsdata = JDBCUtil.query(sql)
//
//    dsdata.foreach(x => {
//      val lrconf: LabelConf = new LabelConf()
//      val diid = x.get("diid").get
//
//      lrconf.set("lrid", x.get("id").get)
//      lrconf.set("name", x.get("name").get)
//      lrconf.set("classname", x.get("class_name").get)
//      lrconf.set("diid", diid)
//      lrconf.set("status", x.get("status").get)
//
//      val propsJsonStr = x.get("properties").getOrElse(null)
//
//      if (propsJsonStr != null) {
////        val propsMap = Json4sUtils.jsonStr2Map(propsJsonStr)
//        val propsArrMap = Json4sUtils.jsonStr2ArrMap(propsJsonStr, "props")
//        propsArrMap.foreach(kvMap => {
//          if (!kvMap.isEmpty) lrconf.set(kvMap.get("name").get, kvMap.get("value").get)
//        })
//      }
//
//	    diid2LabelId2LabelRuleConfMap.getOrElseUpdate(diid, mutable.Map[String, LabelConf]()).put(lrconf.get("lrid"), lrconf)
////      if(!diid2LabelId2LabelRuleConfMap.contains(diid)){
////        diid2LabelId2LabelRuleConfMap.put(diid, mutable.Map[String, LabelRuleConf]())
////      }
////      val labelRuleConfsMap = diid2LabelId2LabelRuleConfMap.get(diid).get
////
////      labelRuleConfsMap.put(lrconf.get("lrid"), lrconf)
//    })
//  }
//
//
//  /**
//   * init eventRule config
//   */
//  def initEventRuleConf() {
//    val sql = "select id, name, EVENT_EXPR, peventid, diid " +
//            "from " + TableInfoConstant.EventTableName +
//            " where status = 1"
//    val dsdata = JDBCUtil.query(sql)
//
//    dsdata.foreach(x => {
//      val erconf: EventConf = new EventConf()
//      val diid = x.get("diid").get
//      val erid = x.get("id").get
//
//      erconf.set("erid", erid)
//      erconf.set("name", x.get("name").get)
//      erconf.set("filter_expr", x.get("filter_expr").get)
//      erconf.set("select_expr", x.get("select_expr").get)
//
//      val peventid = x.get("peventid").get match {
//        case null => "-1"
//        case "" => "-1"
//        case str :String => str
//      }
//      erconf.set("peventid", peventid)
//      erconf.set("diid", diid)
//
//      eventId2DIidMap.put(erid, diid)
//
//      //构造 diid2EventId2EventRuleConfMap
//	    diid2EventId2EventRuleConfMap.getOrElseUpdate(diid, mutable.Map[String, EventConf]()).put(erid, erconf)
////      if(!diid2EventId2EventRuleConfMap.contains(diid)){
////        diid2EventId2EventRuleConfMap.put(diid, mutable.Map[String, EventRuleConf]())
////      }
////      diid2EventId2EventRuleConfMap.get(diid).get.put(erid, erconf)
//      
//      //构造 diid2EventId2ParentEventId
//	    diid2EventId2ParentEventId.getOrElseUpdate(diid, mutable.Map[String, String]()).put(erid, peventid)
////      if(!diid2EventId2ParentEventId.contains(diid)){
////        diid2EventId2ParentEventId.put(diid, mutable.Map[String, String]())
////      }
////      diid2EventId2ParentEventId.get(diid).get.put(erid, peventid)
//      
//    })
//  }
//
//
//
//  /**
//   * init businessEvent config
//   */
//  def initBusinessEventConf() {
//    val sql = "select id, name, properties " +
//            " from " + TableInfoConstant.BusinessEventTableName +
//            " where status = 1"
//    val dsdata = JDBCUtil.query(sql)
//
//    val businessEvents = mutable.ArrayBuffer[SubjectConf]()
//
//    dsdata.foreach(x => {
//      //每行记录对应一个业务配置
//      val beConf: SubjectConf = new SubjectConf()
//
//      val beid = x.get("id").get
//      beConf.set("beid", beid)
//      beConf.set("name", x.get("name").get)
////      beConf.set("classname", x.get("class_name").get)
//
//      val propsJsonStr = x.get("properties").getOrElse(null)
//
//      // 业务配置（）
//      if(propsJsonStr == null) {
//        logger.warn("found properties is null in " + TableInfoConstant.BusinessEventTableName)
//      } else {
//        val propsMap = Json4sUtils.jsonStr2ArrMap(propsJsonStr, "props")
//        propsMap.foreach(kvMap => {
//          if (!kvMap.isEmpty) beConf.set(kvMap.get("pname").get, kvMap.get("pvalue").get)
//        })
//
//        // 业务对应的事件规则配置
//        val eventsArrMap = Json4sUtils.jsonStr2ArrMap(propsJsonStr, "events")
//        val eventId2SelectExpr = mutable.Map[String, String]()
//        val eventIdSelectExprArr = mutable.ArrayBuffer[(String, String)]()
//
//        val events = mutable.ArrayBuffer[String]()
//        eventsArrMap.foreach(kvMap => {
//          val eventId = kvMap.get("eventId").get
//          eventId2SelectExpr.put(eventId, kvMap.get("select_expr").get)
//          eventIdSelectExprArr.append((eventId, kvMap.get("select_expr").get))
//
//          events.append(eventId)
//
//          // 记录每个biid对应的业务
//          val diid = eventId2DIidMap.get(eventId).get
//	        diid2BeidsMap.getOrElseUpdate(diid, mutable.Set[String]()).add(beid)
////          if (!diid2BeidsMap.contains(diid)) diid2BeidsMap.put(diid, mutable.Set[String]())
////          val beidSet = diid2BeidsMap.get(diid).get
////          beidSet.add(beid)
//        })
//        beid2EventId2SelectExprMap.put(beid, eventId2SelectExpr)
//        beid2EventIdSelectExprArrMap.put(beid, eventIdSelectExprArr)
//
//        beid2EventIdsMap.put(beid, events)
//
//
//        // 业务对应的输出数据接口配置，每个业务一个输出事件接口
//        val outputDIidsArrMap = Json4sUtils.jsonStr2ArrMap(propsJsonStr, "output_dis")
//        val outputDIidSet = mutable.Set[String]()
//        outputDIidsArrMap.foreach(kvMap => {
//          outputDIidSet.add(kvMap.get("pvalue").get)
//        })
//        beid2OutputDIidsMap.put(beid, outputDIidSet)
//
//      }
//
//      beid2BeconfMap.put(beid, beConf)
//    })
//  }
// deteled by surq at 2015.11.21 end
}
