package com.asiainfo.ocdp.streaming.tools

import scala.collection.mutable.ArrayBuffer

/**
 * Created by leo on 10/13/15.
 */
object DropWhileTest {
  def main(args: Array[String]) {
    val a = new ArrayBuffer[(String, String)]()
    a.append(("a","a1"),("b","b1"))
    val b = a.dropWhile(_._1.equals("a"))
    a.foreach(println(_))
    b.foreach(println(_))
  }
}
