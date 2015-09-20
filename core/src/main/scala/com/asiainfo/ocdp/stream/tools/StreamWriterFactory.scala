package com.asiainfo.ocdp.stream.tools

import com.asiainfo.ocdp.stream.config.DataInterfaceConf
import com.asiainfo.ocdp.stream.constant.DataSourceConstant

/**
 * Created by leo on 9/18/15.
 */
object StreamWriterFactory {
  def getWriter(diConf: DataInterfaceConf): StreamWriter = {
    diConf.dsConf.dsType match {
      case DataSourceConstant.KAFKA_TYPE => new StreamKafkaWriter(diConf)
      case DataSourceConstant.JDBC_TYPE => new StreamJDBCWriter(diConf)
      case _ => throw new Exception("DataSource type " + diConf.dsConf.dsType + " is not supported !")
    }
  }
}
