package cn.etl

import java.lang

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
  * @Auther: juanjie
  * @Date: 2019/4/2 14:17
  * @Description:
  */
object BasicWork {
  def basicWork(ssc:StreamingContext) = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadp01:9092,hdp02:9092,hdp03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "t222",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    val topics = Array("cmcc")
    //从kafka获取数据
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )
    stream
  }
}
