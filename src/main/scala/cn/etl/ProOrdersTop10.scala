package cn.etl

import java.sql.{Connection, DriverManager}

import com.alibaba.fastjson.JSON
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Auther: juanjie
  * @Date: 2019/4/2 16:36
  * @Description:
  *              充值订单省份top10
  */
object ProOrdersTop10 {
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
    //获取配置文件
    val config = ConfigFactory.load()

    val ssc = new StreamingContext(conf,Seconds(2))
    ssc.checkpoint("./check")
    //省份的code 和省份的名称
    val broPro = ProBroad.proBroad(ssc)
    val stream = BasicWork.basicWork(ssc)

      //过滤出充值订单
      val rechargeOrders = stream.map(crd => JSON.parseObject(crd.value()))
        .filter(it => it.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq")).cache()

      val basicData = rechargeOrders.map(t => {

      //获取省份
      val provinceCode = t.getString("provinceCode")
      val pro = broPro.value
      val province = pro.getOrElse(provinceCode, provinceCode)
      //充值成功的数据  充值成功数  失败数量
      val success = if (t.getString("bussinessRst").equals("0000")) List(1,1, 0) else List(1,0, 1)
      (province, success)
    })

      val value = basicData.map(tp=>(tp._1,tp._2(1))).updateStateByKey(updateFunc).foreachRDD(rdd=>{
      val url=config.getString("db.url")
      val user=config.getString("db.user")
      val password=config.getString("db.password")
      var conn:Connection=null
        val res = rdd.sortByKey()
      res.foreachPartition(rdd=>{
        try {
        conn=DriverManager.getConnection(url,user,password)
        rdd.foreach(t=>{
          //开启事务
          conn.setAutoCommit(false)
          val table =config.getString("db.table")
          val upPstm = conn.prepareStatement(s"insert into ${table} values (?,?)")
          upPstm.setString(1,t._1)
          upPstm.setInt(2,t._2)
          upPstm.executeUpdate()

          /*val upPstm2 = conn.prepareStatement("insert into ProRate values (?,?,?)")
          upPstm.setString(1,t._1)
          upPstm.setInt(2,t._2)
          upPstm.setDouble(3,(t._2.toDouble))*/


          conn.commit()
        })
        }
        catch {
          case e:Exception=>e.printStackTrace()
        }finally {
          conn.close()
        }
      })


    })

    ssc.start()
    ssc.awaitTermination()
  }
}
