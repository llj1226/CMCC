package cn.etl

import java.sql.{Connection, DriverManager}

import cn.tool.{DateUtils, JedisPoolUtils}
import com.alibaba.fastjson.JSON
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Auther: juanjie
  * @Date: 2019/4/2 14:11
  * @Description:
  *              全国各省充值充值业务失败量分布
  */
object ProvinceFair {
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
    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint("./check")
    val broadPro:Broadcast[collection.Map[String,String]]= ProBroad.proBroad(ssc)
    val stream = BasicWork.basicWork(ssc)
    //      val ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    val basicData = TupleData.tupleData(stream,broadPro)
    val fairSum = basicData.map(tp => {
      val hour = tp._3
      val province = tp._4
      ((hour, province), tp._2._5)
    }).filter(_._2!=0).updateStateByKey(updateFunc)
    //订单总量
    fairSum.foreachRDD(rdd=>{
      rdd.foreachPartition(part=> {
        val url = config.getString("db.url")
        val user = config.getString("db.user")
        val password = config.getString("db.password")
        var conn: Connection = null
        // val sumOrders = part.map(_._2._1).sum
        try {
          conn = DriverManager.getConnection(url, user, password)
          part.foreach(t => {
            //开启事务
            conn.setAutoCommit(false)
            val upPstm = conn.prepareStatement("replace into FairProvince values (?,?)")
            upPstm.setString(1, t._1._1 + "--" + t._1._2)
            upPstm.setInt(2, t._2)
            upPstm.executeUpdate()
            conn.commit()
          })
        }
        catch {
          case e: Exception => e.printStackTrace()
        } finally {
          conn.close()
        }
        /*val jedis = JedisPoolUtils.getJedis()
        jedis.incrBy("sumOrders2",sumOrders.toInt)
        jedis.close()
      })
    })*/
        // val sumOrders = basicData.map(_._2._1).sum()
        //        val jedis = JedisPoolUtils.getJedis()
        //        jedis.incrBy("sumOrders",sumOrders.toInt)
        //        jedis.close()

        /*   fairSum.foreachRDD(rdd => {
      rdd.foreachPartition(part => {
        //val jedis = JedisPoolUtils.getJedis()
        part.foreach(t => {
         // if (t._2 != 0)
           // jedis.hincrBy("fairSum", t._1._1 + "--" + t._1._2, t._2)
        })
        //jedis.close()
      })


    })*/
      })
      })
    //sc.stop()
    ssc.start()
    ssc.awaitTermination()
  }
}
