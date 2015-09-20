/*
package com.asiainfo.ocdp.streaming.datasource

import java.util.concurrent.FutureTask

import com.asiainfo.ocdp.streaming.business.BusinessEvent
import com.asiainfo.ocdp.streaming.common.{CodisCacheManager, StreamingCache}
import com.asiainfo.ocdp.streaming.config.{DataInterfaceConf, MainFrameConf}
import com.asiainfo.ocdp.streaming.constant.EventConstant
import com.asiainfo.ocdp.streaming.event.Event
import com.asiainfo.ocdp.streaming.label.Label
import com.asiainfo.ocdp.streaming.tools.CacheFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.slf4j.LoggerFactory

import scala.collection.mutable.{ArrayBuffer, Map}
import scala.collection.{immutable, mutable}

class DataInterface extends Serializable with org.apache.spark.Logging {

	val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  val LABEL_PATTERN = """labels\['(.*)'\]\['(.*)'\]""".r

  var id: String = null
  var conf: DataInterfaceConf = null
  var shuffleNum: Int = 0

  var uksStr: String = null
  var ukSeparator: String = null

  protected val labelRules = new ArrayBuffer[Label]
  protected val eventRules = new ArrayBuffer[Event]
  protected val bsEvents = new ArrayBuffer[BusinessEvent]

  var schema: StructType = _

//  def addEventRule(rule: EventRule): Unit = {
//    eventRules += rule
//  }

  def addLabelRule(rule: Label): Unit = {
    labelRules += rule
  }

  def addBsEvent(bs: BusinessEvent): Unit = {
    bsEvents += bs
  }

  def init(conf: DataInterfaceConf): Unit = {
    this.conf = conf
    id = this.conf.get("diid")
    uksStr = conf.get("uniqKeys")
    ukSeparator = conf.get("UKSeparator")
    schema = MainFrameConf.diid2SchemaMap.get(id).get
  }

  final def readSource(ssc: StreamingContext): DStream[String] = {
    StreamingInputReader.readSource(ssc, conf)
  }

  //日志与Row字段1对1转换的简单实现，
  // 如果需要进行字段变换或新增字段，则可以自定义数据接口类(extend DataInterface)，重写该方法；也可以在标签增强的方式实现
  protected  def transform(source: String, schema: StructType): Option[GenericMutableRow] = {
    val delim = conf.get("delim", ",")
    val inputArr = (source + delim +"DummySplitHolder").split(delim).dropRight(1)
//            .map(_.asInstanceOf[Any])
    if (inputArr.length == conf.getInt("field.numbers")) {

      //TODO：根据 StuctType 配置进行类型转换，避免spark-sql获取列时提示java.lang.ClassCastException
      val dataTypeArr = schema.fields.map(_.dataType.typeName)
      val inputAnyArr = inputArr.zipWithIndex.map{ case (value, idx) =>
        dataTypeArr(idx).toLowerCase match {
//          case "byte" => value.toByte
          case "short" => value.toShort
          case "int" => value.toInt
          case "integer" => value.toInt
          case "long" => value.toLong
          case "float" => value.toFloat
          case "double" => value.toDouble
          case "string" => value
//          case "array[byte]" => BinaryType
          case "boolean" => value.toBoolean
//          case "java.sql.timestamp" => TimestampType
//          case "java.sql.date" => DateType
          case x => throw new Exception("unsupported type convertion for string to " + x)
        }
      }

      Some(new GenericMutableRow(inputAnyArr ++ mutable.Map[String, mutable.Map[String, String]]()))
    } else None
  }

  final def process(ssc: StreamingContext) = {
    val sqlContext = new SQLContext(ssc.sparkContext)

    //1 根据输入数据接口配置，生成数据流 DStream
    val inputStream = readSource(ssc)

    //1.2 根据输入数据接口配置，生成构造 sparkSQL DataFrame 的 structType
    val inputSchmea = MainFrameConf.diid2SchemaMap.get(id).get

    //2 流数据处理
    inputStream.foreachRDD(rdd =>{
      if (rdd.partitions.length > 0) {
        //2.1 流数据转换
        val sourceRDD = rdd.map(inputArr => {
          transform(inputArr, schema)
        }).collect {
          case Some(row) => row
        }

/*
	      //TODO: 处理打标签阶段更新报错
        println("[DEBUG]D[inputSchmea]"+"= = " * 20 + "inputSchmea = ")
        inputSchmea.printTreeString()
        val inputDataFrame = sqlContext.createDataFrame(sourceRDD.map(_.asInstanceOf[Row]), inputSchmea)
        //TODO: 支持过滤
//        val filterdDF = inputDataFrame.filter("")
//        val filterdDF = inputDataFrame.filterNot("")
//        if (sourceRDD.partitions.length > 0) {
	      println("[DEBUG]D[inputDataFrame]" + "= = " * 20 + "inputDataFrame.count = " + inputDataFrame.count())
	      inputDataFrame.show()

          val labeledRDDRow = execLabelRule(inputDataFrame: DataFrame, schema)
*/
	        val labeledRDDRow = execLabelRule(sourceRDD, inputSchmea)
          if (labeledRDDRow.partitions.length > 0) {
//            val df = sqlContext.createDataFrame(labeledRDDRow, inputSchmea)
	          val df = sqlContext.createDataFrame(labeledRDDRow.map(_.asInstanceOf[Row]), inputSchmea)
//	          println("D[df]"+"= = " * 20 + "df.count = " + df.count())
//	          df.show()

	          // cache data
            df.persist

            val eventMap = makeEvents(df)
	          //For DEBUG
//	          println("[DEBUG]D[eventMap]" +"= = " * 20 +"eventMap.count = " + eventMap.size)
//	          for(eventData<- eventMap){
//		          println("[DEBUG]D[event]"+"- -" * 20 + "eventID = " + eventData._1 +", eventData.count = " + eventData._2.count())
//		          eventData._2.show()
//	          }

            subscribeEvents(eventMap)

            df.unpersist()
          }
//        }
      }
    })
  }

  final  def subscribeEvents(eventMap: mutable.Map[String, DataFrame]) {
    println(" Begin subscribe events : " + System.currentTimeMillis())
    if (eventMap.size > 0) {

      eventMap.foreach(x => {
        x._2.persist()
      })

      val bsEventIter = bsEvents.iterator

      while (bsEventIter.hasNext) {
        val bsEvent = bsEventIter.next
        //println("= = " * 20 +"bsEvent.id = " + bsEvent.id)
        //业务订阅事件
        bsEvent.subcribeEvent(eventMap)
      }

      eventMap.foreach(x => {
        x._2.unpersist()
      })
    }
  }

  final def makeEvents(df: DataFrame) = {
    val eventMap: mutable.Map[String, DataFrame] = mutable.Map[String, DataFrame]()
    println(" Begin exec evets : " + System.currentTimeMillis())

    //TODO: 目前支持的依赖类型: and，不支持or
	  val eventId2ParentEventIdMap = MainFrameConf.diid2EventId2ParentEventId.get(id).get
	  val eventId2EventRuleConfMap = MainFrameConf.diid2EventId2EventRuleConfMap.get(id).get

    // 获取event的依赖关系 list 爷爷0－>父1－>本身2
    def getEventDependcy(eventID: String, eventIdList: ArrayBuffer[String]): ArrayBuffer[String] = {
      if (!(eventId2ParentEventIdMap(eventID) == "-1" || eventId2ParentEventIdMap(eventID).isEmpty))
        getEventDependcy(eventId2ParentEventIdMap(eventID), eventIdList)
      eventIdList += eventID
    }
    val events = eventId2EventRuleConfMap.map(eventId2EventRuleConf => {
      val (eventId, eventRuleConf) = eventId2EventRuleConf
      // 存放event依赖的ID list 本身0－>父1－>爷爷2
      val dependciesList = new ArrayBuffer[String]
      getEventDependcy(eventId, dependciesList)
      // 如果eventId已经存在表示已经计算过,则从比结点到最终子孙结点都将忽略
      for (index <- 0 until dependciesList.size if (!eventMap.isDefinedAt(eventId))) {
        val id = dependciesList(index)
        // 如果已经计算过,则忽略
        if (!eventMap.isDefinedAt(id)) {
          var filteredData: DataFrame = null
          // 最基层的信赖
	        println("[DEBUG]D[filter]" +" * - " * 20 +" filterExpr=" + eventRuleConf.filterExpr)
          if (index == 0) filteredData = df.filter(eventRuleConf.filterExpr)
          // 在父信赖的基础上做filter
          else filteredData = eventMap(dependciesList(index - 1)).filter(eventRuleConf.filterExpr)
          // 向eventMap保存每个event的结果集
          eventMap.put(id, filteredData)
        }
      }
    })

    //TODO: 缓存每个事件对应的数据集合，可以不存储与业务/主题事件无直接关系的父事件的数据集
    saveEventData(eventMap)
    eventMap
  }

  // 缓存每个事件对应的数据集合
  //  选择在此处存储:
  final def saveEventData(eventDataMap: mutable.Map[String, DataFrame]): Unit = {
    eventDataMap.foreach { case (eventId, eventData)=>
	    val eventCacheKeyNames = uksStr

//	    logger.debug("[DEBUG]D[saveEventData]" +"- - " * 20 +"eventData.count=" + eventData.count())
	    eventData.show()
      eventData.mapPartitions(iter =>{
        new Iterator[Row] {
          private[this] var current: Row = _
          private[this] var currentPos: Int = -1
          private[this] val batchArrayBuffer = new ArrayBuffer[(String, String, Row)]()

          override def hasNext: Boolean = {
            iter.hasNext && batchNext()
//	          val flag = (currentPos != -1 && currentPos < batchArrayBuffer.length) || (iter.hasNext && batchNext())
//	          flag
          }

          override def next(): Row = {
            batchArrayBuffer.head._3
          }

          var numBatches = 0
          var batchSize = 0
          val batchLimit = MainFrameConf.systemProps.getInt("cacheQryBatchSizeLimit")

          //批量处理
          def batchNext(): Boolean = {
            var result = false

            batchArrayBuffer.clear()

            //获取一个批次处理的row
            while (iter.hasNext && (batchSize < batchLimit)) {
              current = iter.next()
//              val eventKeyValue = current.getAs[String](eventCacheKeyName)
	            val eventKeyValue = eventCacheKeyNames.split(",").map(fieldName=>{
		            current.getAs[String](fieldName)
	            }).mkString(ukSeparator)
	            batchArrayBuffer.append((EventConstant.EVENT_CACHE_PREFIX_NAME + eventKeyValue,
	              EventConstant.EVENTCACHE_FIELD_ROWEVENTID_PREFIX_KEY + eventId, current))

              batchSize += 1
              currentPos += 1
            }

            //构建一个保存线程，提交一个批次的数据
            val taskMap = Map[Int, FutureTask[String]]()
            var index = 0
            if (batchArrayBuffer.length > 0) {
              result = true

              CacheFactory.getManager.asInstanceOf[CodisCacheManager].setEventData(batchArrayBuffer.toArray)

              batchSize = 0
              numBatches += 1
            }
            result
          }
        }
      }).count()
    }
  }

  def execLabelRule(sourceDataFrame: RDD[GenericMutableRow], schema: StructType) = {

    println(" Begin exec labes : " + System.currentTimeMillis())
    val labelRuleArray = labelRules.toArray
    val batchLimit = MainFrameConf.systemProps.getInt("cacheQryBatchSizeLimit")

	  sourceDataFrame.mapPartitions(iter => {
      new Iterator[GenericMutableRow] {
        private[this] var currentRow: GenericMutableRow = _
        private[this] var currentPos: Int = -1
        private[this] var arrayBuffer: Array[GenericMutableRow] = _

        override def hasNext: Boolean = {
          val flag = (currentPos != -1 && currentPos < arrayBuffer.length) || (iter.hasNext && fetchNext())
          flag
        }

        override def next(): GenericMutableRow = {
          currentPos += 1
          arrayBuffer(currentPos - 1)
        }

        private final def fetchNext(): Boolean = {
          val currentArrayBuffer = new ArrayBuffer[GenericMutableRow]
          currentPos = -1
          var totalFetch = 0
          var result = false

          val totaldata = mutable.MutableList[GenericMutableRow]()
          val minimap = mutable.Map[String, GenericMutableRow]()

          val labelQryKeysSet = mutable.Set[String]()

          while (iter.hasNext && (totalFetch < batchLimit)) {
            val currentLine = iter.next()
            totaldata += currentLine

            //TODO: 在标签应用前需要使用的情况会报错
            val uk = uksStr.split(",").map(fieldName=>{
              getFieldValueByNameInRow(currentLine, schema, fieldName)
            }).mkString(ukSeparator)

            minimap += ("Label:" + uk -> currentLine)

//	          logger.debug("[DEBUG]E[labelRuleArray] labelRuleArray.count = " + labelRuleArray.size +", labelRuleArray = " + labelRuleArray.map(_.conf.get("classname")).mkString("[", ",", "]"))
            labelRuleArray.foreach(labelRule => {
              val labelId = labelRule.conf.get("lrid")
              val qryKeys = labelRule.getQryKeys(currentLine, schema)
              if (qryKeys != null && qryKeys.nonEmpty) {
                labelQryKeysSet ++= qryKeys
              }
            })

            totalFetch += 1
            currentPos = 0
            result = true
          }

          println(" partition data size = " + totalFetch)

          val f1 = System.currentTimeMillis()
          var cachemap_old: Map[String, Any] = null
          try {
            cachemap_old = CacheFactory.getManager.getMultiCacheByKeys(minimap.keys.toList)
//            logger.debug("[DEBUG]E[cachemap_old]  cachemap_old.count = " + cachemap_old.size +" cachemap_old.keys = " + cachemap_old.keys.mkString("[", ",", "]"))
          } catch {
            case ex: Exception =>
              logError("= = " * 15 + " got exception in EventSource while get cache")
              throw ex
          }
          val f2 = System.currentTimeMillis()
          println(" query label cache data cost time : " + (f2 - f1) + " millis ! ")

          val labelQryData = CacheFactory.getManager.hgetall(labelQryKeysSet.toList)

//	        logger.debug("[DEBUG]E[labelQryData]  labelQryData.count = " + labelQryData.size +", labelQryKeysSet = " + labelQryKeysSet.mkString("[", ",", "]"))
//	        labelQryData.foreach(kMap=>{
//		        println("[DEBUG]E[labelQryData] for " + kMap._1 +", Map = " + kMap._2.mkString("[", ",", "]"))
//	        })

          val f3 = System.currentTimeMillis()
          println(" query label need data cost time : " + (f3 - f2) + " millis ! ")

          val cachemap_new = mutable.Map[String, Any]()
          totaldata.foreach(x => {
            val uk = uksStr.split(",").map(field=>{
	            x.getAs[String](DataInterface.getFieldIdx(schema, field.trim))
            }).mkString(ukSeparator)

            val key = "Label:" + uk
            val value = x

            var rule_caches = cachemap_old.get(key).get match {
              case cache: immutable.Map[String, StreamingCache] => cache
              case null =>
	              val cachemap = mutable.Map[String, StreamingCache]()
	              labelRuleArray.foreach(labelRule => {
	                cachemap += (labelRule.conf.get("lrid") -> null)
	              })
	              cachemap.toMap
            }

            labelRuleArray.sortBy(_.conf.get("lrid")).foreach(labelRule => {

              val cacheOpt = rule_caches.get(labelRule.conf.get("lrid"))
              var old_cache: StreamingCache = null
              if (cacheOpt != None) old_cache = cacheOpt.get

              val newcache = labelRule.attachLabel(value, schema, old_cache, labelQryData)
              rule_caches = rule_caches.updated(labelRule.conf.get("lrid"), newcache)

            })
            currentArrayBuffer.append(value)

            cachemap_new += (key -> rule_caches.asInstanceOf[Any])
          })

          val f4 = System.currentTimeMillis()
          println(" Exec labels cost time : " + (f4 - f3) + " millis ! ")

          //update caches to CacheManager
          CacheFactory.getManager.setMultiCache(cachemap_new)
          println(" update labels cache cost time : " + (System.currentTimeMillis() - f4) + " millis ! ")

          arrayBuffer = currentArrayBuffer.toArray
          result
        }
      }
    })
  }
  
  
  //获取特定形式 Row(x, y, z, ..., mutable.Map[String, mutable.Map[String, String]]) 的指定列名的值
  def getFieldValueByNameInRow(row: Row, schema: StructType, fieldName: String): String ={
    if (fieldName.startsWith("labels[")) {
      val LABEL_PATTERN(k1, k2) = fieldName
      row.getAs[mutable.Map[String, mutable.Map[String, String]]](row.length - 1).get(k1).get(k2)
    } else {
      val fieldIdx = schema.fieldIndex(fieldName)
      row.getAs[String](fieldIdx)
    }
  }

}


object DataInterface {

	val LABEL_PATTERN = """labels\['(.*)'\]\['(.*)'\]""".r

	/**
	 * 已知schema，更新row中指定列名的值，其中schema最后一列的类型是 mutable.Map[String, mutable.Map[String, String]
	 * @param row
	 * @param schema
	 * @param fieldName
	 * @param value
	 */
	def updateRowWithFieldName(row: GenericMutableRow, schema: StructType, fieldName: String, value: Any): Unit ={
		if (fieldName.startsWith("labels[")){
			val LABEL_PATTERN(k1, k2) = fieldName
			val outerMap = row.getAs[mutable.Map[String, mutable.Map[String, String]]](row.length-1)

			outerMap.get(k1) match {
				case Some(innerMap) => innerMap.put(k2, value.asInstanceOf[String])
				case None =>
					val innerMap = mutable.Map[String, String]()
					innerMap.put(k2, value.asInstanceOf[String])
				  outerMap.put(k1, innerMap)
			}
		} else {
			row.update(schema.fieldIndex(fieldName), value)
		}
	}

	def getFieldIdx(schema: StructType, fieldName: String): Int = {
		val fieldName2fieldIndexMap = schema.fieldNames.zipWithIndex.toMap
		if (fieldName.startsWith("labels")){
			fieldName2fieldIndexMap.size - 1
		} else {
			fieldName2fieldIndexMap.get(fieldName).get
		}
	}
}*/
