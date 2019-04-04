package cn.tool

import java.text.SimpleDateFormat

/**
  * @Auther: juanjie
  * @Date: 2019/4/2 11:39
  * @Description:
  */
object DateUtils {
  def cacilateTime(startTime:String,endTime:String)={
    val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    val startDate = format.parse(startTime).getTime
    val endDate = format.parse(endTime).getTime
    val time = endDate - startDate
    time /60 /60
  }
}
