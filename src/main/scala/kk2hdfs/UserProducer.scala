package kk2hdfs

import java.io.File
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import scala.io.Source

/**
  * @ author: create by LuJuHui
  * @ date:2018/8/2
  */
object UserProducer {
  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.setProperty("bootstrap.servers", "manager:9092,namenode:9092,datanode:9092");
    val producter = new KafkaProducer[String, String](props, new StringSerializer(), new StringSerializer());

    /**
      * user_friends.csv文件格式如下
      * user,friends...
      * 3197468391,1346449342 3873244116 4226080662 1222907620 547730952 1052032722 2138119761
      * 3537982273,1491560444 395798035 2036380346 899375619 3534826887 3427911581 494959696
      * 823183725,1484954627 1950387873 1652977611 4185960823 427135664 3046862298 3536710376
      */
    Source.fromFile(new File("E:\\Projects\\kafka_user\\src\\main\\resources\\my_friends.csv")).getLines().foreach(line => {
      val record = new ProducerRecord[String, String]("user_friend", line.toString);
      println(line)
      Thread.sleep(2)
      producter.send(record);
    })
  }
}
