package com.asiainfo.ocdp.stream.subject

import com.asiainfo.ocdp.stream.manager.StreamTask
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.StreamingContext

/**
 * Created by leo on 9/16/15.
 */
class SubjectTask(id: String, interval: Int) extends StreamTask {

  final def process(ssc: StreamingContext) = {
    this.ssc = ssc
    sqlc = new SQLContext(ssc.sparkContext)

    //1 根据输入数据接口配置，生成数据流 DStream
    val inputStream = readSource(ssc)

    //1.2 根据输入数据接口配置，生成构造 sparkSQL DataFrame 的 structType
    val schema = conf.getBaseSchema
    val udfSchema = conf.getUDFSchema

    //2 流数据处理
    inputStream.foreachRDD(rdd => {
      if (rdd.partitions.length > 0) {
        //2.1 流数据转换

        val rowRDD = rdd.map(inputArr => {
          transform(inputArr, schema)
        }).collect {
          case Some(row) => row
        }

        val df: DataFrame = sqlc.createDataFrame(rowRDD, schema)

        val colarr: Array[String] = schema.fieldNames.union(udfSchema.fieldNames)

        val mixDF = df.filter(conf.get("filter_expr", "1=1")).selectExpr(colarr: _*)

        val enhancedDF = execLabels(mixDF)

        enhancedDF.persist

        makeEvents(enhancedDF, conf.get("uniqKeys"))

        //        subscribeEvents(eventMap)

        enhancedDF.unpersist()

      }
    })
  }

}
