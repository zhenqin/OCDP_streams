package com.asiainfo.ocdp.streaming.tools

import java.text.SimpleDateFormat
import java.util.Date

import com.asiainfo.ocdp.stream.tools.Json4sUtils
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

/**
  *
  * @author Rayn on 2016/10/16.
  * @email liuwei412552703@163.com.
  */
object ReduceByKeyTest extends App {

//  val list = Array("one", "two", "three", "four", "five")
//  private val toSeq: Seq[Array[Byte]] = list.map(x => x.getBytes).toSeq
//  println(toSeq)
//
//
//  val jsonStrs = "{\"msg\":\"test\"}"
//
//  val jsonStr_target = compact(parse(jsonStrs) \ "msg")
//  println(jsonStr_target)
//
//  implicit val formats = DefaultFormats
//  private val mapJSON: Map[String, String] = parse(jsonStrs).extract[Map[String, String]]
//  println(mapJSON)
//
//
//
//  def reduceByKeyFun = (jsonLabelStr1:String, jsonLabelStr2:String) => {
//    if(jsonLabelStr1 > jsonLabelStr2) jsonLabelStr1
//    else jsonLabelStr2
//  }
//
//  private val byKeyFun: String = reduceByKeyFun("123123,20150910", "123123,20150909")
//  println(byKeyFun)

  private val date: Date = new Date(1476676932075L)
  private val current_time: Long = System.currentTimeMillis()
  val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

  println(simpleDateFormat.format(date))
  println(simpleDateFormat.format(current_time))


  println(2592000 * 1000L)
  println(current_time)
  val cache_time = "1476676932075"
  println(("测试执行时间：" + (current_time > (cache_time.toLong +2592000 * 1000L))))


  if (current_time >= (cache_time.toLong + 2592000 * 1000)){
    println("卧槽，，什么情况")
  } else {
    println("他大爷的。。。。。")

  }


  var outPutJsonMap = Map[String, String]()
  outPutJsonMap += ("test" -> "test1")
  outPutJsonMap += ("two" -> "two2")

  private val map: List[String] = outPutJsonMap.toList.map(_._2)
  println(map)


  val ukUnion = "imsi:phone_no".split(":")

  val jsonStr = "{\"phone_no\":\"15991881188\",\"time\":\"20161010 14:25:52.000\",\"callingimei\":\"15991881188\",\"imsi\":\"8637380254346000\"}"
  val currentLine = Json4sUtils.jsonStr2Map(jsonStr)
  println("currentLine : " + currentLine)

  val uk = ukUnion.map(currentLine(_)).mkString(",")
  println("uk : " + uk)

  println("Label:" + uk -> currentLine)

}
