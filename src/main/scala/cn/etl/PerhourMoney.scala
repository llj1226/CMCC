package cn.etl

import cn.etl.PerhourOrders.updateFunc
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

/**
  * @Auther: juanjie
  * @Date: 2019/4/3 10:49
  * @Description:
  *              实时统计每小时的充值笔数和充值金额。
  */
object PerhourMoney {
  Logger.getLogger("org").setLevel(Level.WARN)
  def updateFunc (currentData:Seq[Double],oldData:Option[Double]):Option[Double]={
    val newData=currentData.sum+oldData.getOrElse(0d)
    Some(newData)
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      //设置程序的序列化是kryo的序列化
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //缓慢的关闭jvm
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      //设置streaming kafka每一批次 的最大消费
      .set("spark.streaming.kafka.maxRatePerPartition", "2000")
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(2))
    ssc.checkpoint("./check")
    val stream = BasicWork.basicWork(ssc)
    val broadPro = ProBroad.proBroad(ssc)
    val basicData = TupleData.tupleData(stream,broadPro)
    val SuccNum = basicData.map(t => {
      val hour = t._3
      val money = t._2._3
      (hour, money)
    }).updateStateByKey(updateFunc)
    SuccNum.foreachRDD(rdd=>{
      rdd.foreachPartition(part=>{
        //默认加载db
        DBs.setup()
        part.foreach(t=>{
          DB.autoCommit{implicit session=>
            SQL("replace into SuccMoney values(?,?)").bind(t._1,t._2).update().apply()
          }
        })
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
