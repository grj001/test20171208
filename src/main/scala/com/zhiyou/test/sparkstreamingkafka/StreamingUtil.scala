package com.zhiyou.test.sparkstreamingkafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object StreamingUtil {
  val conf = new SparkConf().setAppName("streaming").setMaster("local[*]")
  val ssc = new StreamingContext(conf,Duration(10000))
  ssc.checkpoint("/btripcheckpoint")
  def getStreamingFromKafka(topics:Array[String]) = {
    //kafka的consumer配置信息
    val kafkaParams = Map[String,String](
      "bootstrap.servers" -> "master:9092,master:9093",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "use_a_separate_group_id_for_each_stream1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> "false"
    )
    KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent
      ,ConsumerStrategies.Subscribe[String,String](topics,kafkaParams))
  }
}
