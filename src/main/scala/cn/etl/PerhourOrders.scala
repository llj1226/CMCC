package cn.etl

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

/**
  * @Auther: juanjie
  * @Date: 2019/4/3 09:00
  * @Description:
  *              实时统计每小时的充值笔数
  */
object PerhourOrders {
  Logger.getLogger("org").setLevel(Level.WARN)
  def updateFunc (currentData:Seq[Int],oldData:Option[Int]):Option[Int]={
    val newData=currentData.sum+oldData.getOrElse(0)
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
      val numSucc = t._2._2
      (hour, numSucc)
    }).updateStateByKey(updateFunc)
    SuccNum.foreachRDD(rdd=>{
      rdd.foreachPartition(part=>{
        //默认加载db.default
        DBs.setup()
        part.foreach(t=>{
          //查询数据并返回个例，并将列数据封装到集合中
          DB.autoCommit{implicit session=>
            SQL("replace into SuccNum values(?,?)").bind(t._1,t._2).update().apply()
          }
        })
      })
    })



    ssc.start()
    ssc.awaitTermination()
  }
}
