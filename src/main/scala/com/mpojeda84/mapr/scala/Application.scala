package com.mpojeda84.mapr.scala

import com.mapr.db.spark.MapRDBSpark
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.kafka09.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Milliseconds, StreamingContext};

object Application {

  def main (args: Array[String]): Unit = {

    val config = new SparkConf().setAppName("Likes Processor")

    val sc = new SparkContext(config)
    implicit val ssc: StreamingContext = new StreamingContext(sc, Milliseconds(500))

    println("### RUNNING ###")

    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](List("/user/mapr/Streams/test2:topic-2"), kafkaParameters)
    val directStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, consumerStrategy)

    directStream
      .map(_.value)
      .map(MapRDBSpark.newDocument)
      .foreachRDD(_.saveToMapRDB("/user/mapr/tables/table2"))

    ssc.start()
    ssc.awaitTermination()
  }

  private def kafkaParameters = Map(
    ConsumerConfig.GROUP_ID_CONFIG -> "aggregators",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true"
  )

}