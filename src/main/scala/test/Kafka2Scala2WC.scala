package test

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
  * @ author: create by LuJuHui
  * @ date:2018/7/31
  */
object Kafka2Scala2WC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Kafka2Scala2WC").setMaster("local[3]")  //local[3]指的是在本地运行，启动3个进程
    val ssc = new StreamingContext(conf, Seconds(5))  //每5秒钟统计一次数据
    val kafkaParams = Map[String, Object](
      /*kafka的端口号*/
      "bootstrap.servers" -> "manager:9092,namenode:9092,datanode:9092",
      /*k-v的反序列化*/
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      /*kafka的组号*/
      "group.id" -> "kafka_wc",
      /*偏移量重置*/
      "auto.offset.reset" -> "latest",
      /*是否自动提交*/
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    /*kafka的已经创建的主题*/
    val topics = Array("realtime")   //主题可以有多个“topicA”，“topicB”

    /*创建一个离散流*/
    val data = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    /*对kafka消费端取出来的数据进行初步处理，获取value值*/
    val lines = data.map(_.value())
    /*对初步处理的数据进行扁平化处理，并按照空格切分*/
    val words = lines.flatMap(_.split(" "))
    /*获取的单词作为key值，‘1’作为value值，组成（K，V）对*/
    val wordAndOne = words.map((_, 1))
    val reduced = wordAndOne.reduceByKey(_ + _)

    /*打印结果*/
    reduced.print()

    /*启动sparkstreaming程序*/
    ssc.start()
    /*等待程序退出*/
    ssc.awaitTermination()

  }
}
