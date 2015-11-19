package com.asiainfo.ocdp.stream.common

import java.sql.{ Connection, DriverManager, ResultSet, Statement }

import com.asiainfo.ocdp.stream.constant.CommonConstant
import scala.collection.mutable.ArrayBuffer
import scala.xml.XML

/**
 * Created by leo on 8/12/15.
 */
object JDBCUtil {

  private def connection: Connection = {
    //    val spark_home = System.getenv("SPARK_HOME")
    //    val xml = XML.loadFile(spark_home + "/conf/" + CommonConstant.DBConfFile)
    val xml = XML.loadFile(CommonConstant.DBConfFile)
    val mysqlNode = (xml \ "db")
    val url = (mysqlNode \ "url").text
    val username = (mysqlNode \ "username").text
    val password = (mysqlNode \ "password").text
    val classname = (mysqlNode \ "classname").text

    Class.forName(classname)
    DriverManager.getConnection(url, username, password)
  }

  def query(sql: String): Array[Map[String, String]] = {
    var statement: Statement = null
    var rs: ResultSet = null
    var result = ArrayBuffer[Map[String, String]]()
    try {
      // Configure to be Read Only
      statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      // Execute Query
      rs = statement.executeQuery(sql)

      // Getting column names
      val md = rs.getMetaData

      // Iterate Over ResultSet
      while (rs.next) {
        val line: Map[String, String] = (1 to md.getColumnCount).map(index => {
          (md.getColumnLabel(index), rs.getString(index))
        }).toMap[String, String]
        result += line
      }
      result.toArray
    } finally {
      if (rs != null) rs.close()
      if (statement != null) statement.close()
      if (connection != null) connection.close()
    }
  }

  def execute(sql: String): Unit = {
    var statement: Statement = null
    try {
      // Configure to be Read Only
      statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      // Execute Query
      statement.execute(sql)
    } finally {
      if (statement != null) statement.close()
      if (connection != null) connection.close()
    }
  }

}
