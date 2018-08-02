package kk2hdfs

import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ author: create by LuJuHui
  * @ date:2018/8/2
  */
object UserTotal {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UserTotal")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    //kafka
    val config = Map[String, String](
      "bootstrap.servers" -> "manager:9092,namenode:9092,datanode:9092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "kafka_hdfs"
    )
    val topics = Set("user_friend")
    //创建Dstream
    val kafka = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, config))
    val lines = kafka.map(_.value())
    val pairs = lines.map(x => {
      var ss = x.split(",")
      var friends = ss(1).split(" ")
      (ss(0), friends.size)
    })
    pairs.foreachRDD(rdd => {
      //将结果存入hdfs
      rdd.saveAsTextFile("hdfs://namenode:9000/user/kafka/spark/data")
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
