package cn.lixian

import java.sql.{Connection, DriverManager}

import cn.tool.DateUtils
import com.alibaba.fastjson.JSON
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Auther: juanjie
  * @Date: 2019/4/3 11:03
  * @Description:
  *              以省份为维度统计每个省份的充值失败数,及失败率存入MySQL中。
  */
object FairPro {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
   val session = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc = session.sparkContext
    //导入sparksession实例上的隐式转换
    import session.implicits._
    val dataFrame = session.read.json("hdfs://hdp01:9000/cmcc")

   /* val frame = dataFrame.select("provinceCode","requestId","chargefee","bussinessRst")
    frame.createTempView("v_tmp")
    session.sql("select provinceCode ")*/






    val data = dataFrame
    val dirctData = session.read.textFile("E:\\千峰视频\\项目\\4月1日\\es\\充值平台实时统计分析\\city.txt")
    val dirctMap = dirctData.rdd.map(t => {
      val split = t.split("\\s")
      val num = split(0)
      val proName = split(1)
      (num, proName)
    }).collectAsMap()
    val dirctBroad = sc.broadcast(dirctMap)
    val basicData = data.map(df => {
      val provinceCode = df.getAs[String]("provinceCode")
      val proRdd = dirctBroad.value
      //省份
      val province = proRdd.getOrElse(provinceCode, provinceCode)
      //分钟
      val minuteTime = df.getAs[String]("requestId").substring(0, 12)
      //小时
      val hourTime = df.getAs[String]("requestId").substring(0, 8)
      //金额
      val money = df.getAs[String]("chargefee").toDouble
      //返回结果是充值笔数，充值成功数，订单金额，充值失败
      val success = if (df.getAs[String]("bussinessRst").equals("0000")) (1, 1, money, 0) else (1, 0, 0d, 1)
      (province, success, minuteTime, hourTime)
    })

    //获取配置文件
    val config = ConfigFactory.load()
    basicData.map(tp => {
      //省份
      val province = tp._1
      //充值笔数
      val count = tp._2._1
      //充值失败数
      val fairCount = tp._2._4
      (province, (count, fairCount))
    }).rdd.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .foreachPartition(part=>{
      val url = config.getString("db.url")
      val user = config.getString("db.user")
      val password = config.getString("db.password")
      var conn: Connection = null
      try {
        conn = DriverManager.getConnection(url, user, password)
        part.foreach(t => {
          //开启事务
          conn.setAutoCommit(false)
          val upPstm = conn.prepareStatement("insert into LxFairPro values(?,?,?)")
          upPstm.setString(1, t._1)
          upPstm.setInt(2,t._2._1 )
          upPstm.setDouble(3,(t._2._2/t._2._1.toDouble))
          upPstm.executeUpdate()
          conn.commit()
        })
      }catch{
        case e:Exception=>e.printStackTrace()
      }finally {
        conn.close()
      }
    })
    session.stop()
  }
}
