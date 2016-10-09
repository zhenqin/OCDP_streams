package com.asiainfo.ocdp.streaming.tools

import scala.collection.SortedMap


/**
  *
  * @author Rayn
  * @email liuwei412552703@163.com
  *        Created by Rayn on 2016/9/30 14:25.
  */
object SparkTest {

  def main(args: Array[String]) {
    var resultMap = SortedMap[String, Int]()
    val proxylist = Array("now-01", "now-02", "now-03", "now-04", "now-05", "now-06", "now-07", "now-08", "now-09")

    for (i <- 1 to 400) {

      val proxyid = new java.util.Random().nextInt(proxylist.size)
      val proxyInfo = proxylist(proxyid)

      val count = resultMap.getOrElse(proxyInfo, 0) + 1
      resultMap += (proxyInfo -> count)
    }

    resultMap.foreach(f => {
      println(f._1 + " ---- " + f._2)
    })
  }

}
