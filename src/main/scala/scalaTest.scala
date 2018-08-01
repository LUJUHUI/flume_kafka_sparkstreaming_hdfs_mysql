import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ author: create by LuJuHui
  * @ date:2018/8/1
  */
object scalaTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("scalaTest").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    val lines = ssc.socketTextStream("manager", 1111)
    val words = lines.flatMap(_.split(" "))
    val wordAndOne = words.map((_, 1))
    val results = wordAndOne.reduceByKey(_ + _)
    results.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
