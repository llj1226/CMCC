package cn.etl

import cn.tool.DateUtils
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.InputDStream

/**
  * @Auther: juanjie
  * @Date: 2019/4/3 09:20
  * @Description:  将数据按照指标组装成元组
  */
object TupleData {
  def tupleData(stream:InputDStream[ConsumerRecord[String,String]],broadPro:Broadcast[collection.Map[String,String]]) = {
    //过滤出充值订单
    val rechargeOrders = stream.map(crd => JSON.parseObject(crd.value()))
      .filter(it => it.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq")).cache()
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
      //获取省份的code
      val province111 = t.getString("provinceCode")
      //充值成功的数据  充值笔数 充值成功数  充值金额 充值时长  失败数量
      val success = if (t.getString("bussinessRst").equals("0000")) (1, 1, chargefee, time, 0) else (1, 0, 0d, 0, 1)
      val proRdd = broadPro.value
      val province = proRdd.getOrElse(province111, province111)
      //日期 成功量  小时  省份的code
      (day, success, hour, province)
    })
    basicData
  }
}
