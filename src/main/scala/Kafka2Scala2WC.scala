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
    val conf = new SparkConf().setAppName("Kafka2Scala2WC").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
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
    val topics = Array("realtime")

    /*创建一个离散流*/
    val data = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val lines = data.map(_.value())
    /*对kafka消费端取出来的数据进行扁平化处理（string，string）-> string*/
    val words = lines.flatMap(_.split(" "))
    val wordAndOne = words.map((_, 1))
    val reduced = wordAndOne.reduceByKey(_ + _)
    reduced.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
