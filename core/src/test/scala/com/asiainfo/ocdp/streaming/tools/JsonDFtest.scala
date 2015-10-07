package com.asiainfo.ocdp.streaming.tools

import java.util.Properties

import com.asiainfo.ocdp.stream.tools.Json4sUtils
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SaveMode, SQLContext, Row}

/**
 * Created by leo on 9/14/15.
 */
object JsonDFtest {

  def main(args: Array[String]) {
    val arr = Seq("zhangsan,18,man", "lisi,20,woman")
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("test")
    val sc = new SparkContext(conf)
    val sqlsc = new SQLContext(sc)

    val rdd = sc.parallelize(arr).map(x => {
      Row.fromSeq(x.split(",").toSeq)
    })

    val fields = Array(StructField("name", StringType), StructField("age", StringType), StructField("sex", StringType))
    val schema = new StructType(fields)

    val df = sqlsc.createDataFrame(rdd, schema)

    val df2 = df.filter("1=1").selectExpr("name","age","sex","case when name='zhangsan' then 'heihie' end as aa")

    df2.printSchema()
    df2.foreach(x => println(x+"@@@@"))

    df2.toJSON.foreach(json => {
      val map = Json4sUtils.jsonStr2Map(json)
      println("name:"+map.get("name").get)
    })

    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "123")
    properties.setProperty("rowId", "false")
    df2.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/ocdp","abc",properties)
  }

}
