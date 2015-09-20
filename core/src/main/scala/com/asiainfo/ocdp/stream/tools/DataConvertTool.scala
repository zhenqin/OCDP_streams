package com.asiainfo.ocdp.stream.tools

/**
 * Created by tsingfu on 15/8/18.
 */
object DataConvertTool {

  /**
   * 返回字符串形式10进制对应的16进制
   * @param in
   * @return
   */
  def convertHex(in:String):String ={
    var lc = Integer.toHexString(in.toInt).toUpperCase
    while(lc.length<4){lc = "0" + lc}
    lc
  }
}
