package com.asiainfo.ocdp.streaming.tools

import scala.collection.mutable.ArrayBuffer

/**
 * Created by leo on 9/17/15.
 */
object ArrayTest {
  def main(args: Array[String]) {
    val a = ArrayBuffer[(String,String)]()

    a += (("9", "5"))
    a += (("3", "5"))
    a += (("21",null))
    a += (("11",null))
    a += (("5","1"))
    a += (("1",null))
    a += (("18",null))

    val b = a.sortWith((x,y) => {
      if(x._2!=null && y._2!=null){
        x._1.compareTo(y._1) < 0
      }else if(x._2==null && y._2!=null){
        if(y._2.equals(x._1)) {true
        }else x._1.compareTo(y._1) < 0

      }else if(x._2!=null && y._2==null){
        if(x._2.equals(y._1)) {true
        }else x._1.compareTo(y._1) < 0
      }else true

    })

    val c = a.sortWith((x,y) => {
      (x._2!=null && x._2.equals(y._1)) || (y._2!=null && y._2.equals(x._1)) || x._1.compareTo(y._1) < 0
    })

    c.foreach(println(_))
  }
}
