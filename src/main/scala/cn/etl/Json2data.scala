package cn.etl

import java.lang

import cn.tool.{DateUtils, JedisPoolUtils}
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Auther: juanjie
  * @Date: 2019/4/1 19:08
  * @Description:1)	统计全网的充值订单量, 充值金额, 充值成功数
  */
object Json2data {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(2))

    val stream = BasicWork.basicWork(ssc)
    val res = stream.foreachRDD(rdd => {
      //      val ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      if (rdd.isEmpty()) {}
        //过滤出充值订单
        val rechargeOrders = rdd.map(crd => JSON.parseObject(crd.value()))
          .filter(it => it.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq")).cache()
        //总订单量
        val ordersSum = rechargeOrders.count()
//        val jedis = JedisPoolUtils.getJedis()
//        jedis.set("ordersSum",ordersSum.toString)
//        jedis.close()
        //充值成功数
        val basicData = rechargeOrders.map(t => {
          //开始时间
          val startTime = t.getString("requestId").substring(0, 17)
          //结束时间
          val receiveNotifyTime = t.getString("receiveNotifyTime")
          //充值时长
          val time = DateUtils.cacilateTime(startTime, receiveNotifyTime)
          //充值金额
          val chargefee = t.getString("chargefee").toDouble
          //充值日期
          val day = startTime.substring(0, 8)
          //每小时
          val hour = t.getString("requestId").substring(0, 10)
          //充值成功的数据  充值成功数  充值金额 充值时长  失败数量
          val success = if (t.getString("bussinessRst").equals("0000")) (1, chargefee, time, 0) else (0, 0d, 0, 1)
          (day, success, hour)
        })

        //每天充值成功的订单金额
//        val totalMoney = basicData.map(it=>(it._1,it._2._2))
        basicData.foreachPartition(part=>{
        val jedis = JedisPoolUtils.getJedis()
          part.foreach(it=>{
            jedis.hincrBy("totalMoney",it._1,it._2._2.toLong)
          })
          jedis.close()
        })
        //充值成功数
        basicData.foreachPartition(part=>{
          val jedis = JedisPoolUtils.getJedis()
          part.foreach(it=>{
           // jedis.hincrBy("successNum",it._1,it._2._1)
          })
          jedis.close()
        })

    })


    ssc.start()
    ssc.awaitTermination()
  }
}
