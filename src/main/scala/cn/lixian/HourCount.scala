package cn.lixian

import java.sql.{Connection, DriverManager}

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @Auther: juanjie
  * @Date: 2019/4/3 15:20
  * @Description:
  *              1)	以省份为维度,统计每分钟各省的充值笔数和充值金额
  */
object HourCount {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local[*]").appName(this.getClass.getSimpleName).getOrCreate()
    val sc = session.sparkContext
    //导入sparksession实例上的隐式转换
    import session.implicits._
    val dataFrame = session.read.json("hdfs://hdp01:9000/cmcc")
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
      val hourTime = df.getAs[String]("requestId").substring(0, 10)
      //金额
      val money = df.getAs[String]("chargefee").toDouble
      //返回结果是充值笔数，充值成功数，订单金额，充值失败
      val success = if (df.getAs[String]("bussinessRst").equals("0000")) (1, 1, money, 0) else (1, 0, 0d, 1)
      (province, success, minuteTime, hourTime)
    })
    //获取配置文件
    val config = ConfigFactory.load()
    basicData.map(tp=>{
      val province = tp._1
      val hour = tp._4
      val count = tp._2._1
      val money = tp._2._3
      ((province+"-"+hour),(count,money))
    }).rdd.reduceByKey((a,b)=>(a._1+b._1,a._2+b._2))
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
              val upPstm = conn.prepareStatement("insert into hourCountAndMoney values(?,?,?)")
              upPstm.setString(1, t._1)
              upPstm.setInt(2,t._2._1 )
              upPstm.setDouble(3,(t._2._2))
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
