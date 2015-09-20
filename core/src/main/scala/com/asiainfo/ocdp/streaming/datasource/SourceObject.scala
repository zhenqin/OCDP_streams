package com.asiainfo.ocdp.streaming.datasource

import scala.collection.mutable

/**
 * Created by tsingfu on 15/8/18.
 */
abstract class SourceObject(val slabels: mutable.Map[String, mutable.Map[String, String]]) extends Serializable {

  def setLabel(key: String, value: mutable.Map[String, String]) = {
    slabels += (key -> value)
  }

  def getLabels(key: String): mutable.Map[String, String] = {
    if (!slabels.contains(key)) {
      setLabel(key, mutable.Map[String, String]())
    }
    slabels.get(key).get
  }

  final def removeLabel(key: String) = {
    slabels.remove(key)
  }

  def generateId: String
}

