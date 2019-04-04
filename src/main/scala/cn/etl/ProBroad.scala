package cn.etl

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext

/**
  * @Auther: juanjie
  * @Date: 2019/4/2 17:17
  * @Description:
  */
object ProBroad {
  def proBroad(ssc:StreamingContext) = {
    val sc: SparkContext = ssc.sparkContext
    val dirctData = sc.textFile("E:\\千峰视频\\项目\\4月1日\\es\\充值平台实时统计分析\\city.txt")
    val splitPro = dirctData.map(t => {
      val split = t.split("\\s")
      val num = split(0)
      val proName = split(1)
      (num, proName)
    }).collectAsMap()
    val broadPro = sc.broadcast(splitPro)
    broadPro
  }
}
