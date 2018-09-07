package meituan.adv

import java.sql.Connection
import java.util.{Date, Properties}

import com.sun.java.swing.plaf.windows.resources.windows
import kafka.serializer.StringDecoder
import meituan.conf.ConfigurationManager
import meituan.constant.Constants
import meituan.jdbc.MysqlPool
import meituan.utils.DateUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
	* @ author: create by LuJuHui
	* @ date:2018/8/18
	*/
object AdvAreaCountSpark {
	def main(args: Array[String]): Unit = {

		/**
			* 第一步:
			* 创建sparksession和sparkcontext
			*/
		val spark = SparkSession.builder()
			.appName(Constants.SPARK_APP_NAME_PRODUCT)
			.config("spark.sql.wareouse.dir", "hdfs://hadoop01:9000/user/hive/warehouse")
			.enableHiveSupport()
			.getOrCreate()

		val ssc: StreamingContext = new StreamingContext(spark.sparkContext.getConf, Seconds(5))

		/**
			* 第二步：
			* 从kafka中读取数据
			*
			* ssc:StreamingContext
			* kafkaParams: Map[String, String]
			* topics: Set[String]
			*
			* K : ClassTag
			* V : ClassTag
			* KD: Decoder[K] : ClassTag
			* VD: Decoder[V] : ClassTag
			*
			* K : 偏移量位置信息
			* V : 我们获取的数据
			*
			*/

		val kafkaParams: Map[String, String] = Map("metadata.broker.list" -> ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST))
		val topics: Set[String] = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS).split(",").toSet

		/** 获取到了日志数据 */
		val logDstream: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)


		/**
			* 第三步:
			* 黑名单过滤
			**/
		val filterDstream: DStream[String] = filterBlickList(logDstream, spark)


		/**
			* 第四步：
			* 根据规则生成黑名单
			*
			* 规则：一个用户  在  一天内  对  一个广告  点击次数  超过 一百次 ，那么这个用户就会进入黑名单
			*
			**/
		generaterDynamicBlackList(filterDstream, spark)

		/**
			*
			* 第五步：
			* 统计每天各省份各城市的广告点击量
			*
			*/

		val date_province_adv2Count_ds = calculatorCityClickCount(filterDstream)


		/**
			* 第六步：
			* 统计各区域热门商品点击的次数  TOP N
			*
			*/


		/**
			* 第七步：
			* 实时统计统计某个阶段广告投放的趋势
			* window
			*/


		ssc.start()
		ssc.awaitTermination()
		ssc.stop()
	}

	/**
		* 第三步：
		* 进行黑名单的过滤
		*
		* 黑名单存储在两个位置
		* a.mysql 关系型数据库
		* b.redis 内存数据库
		*
		**/


	def filterBlickList(logDstream: DStream[String], spark: SparkSession): DStream[String] = {
		/**
			* 黑名单是从持久化系统中读取到的
			* List((1L,true),(2L,true),(3L,true))
			* 其中1L，2L，3L代表的是黑名单中的用户ID号
			**/

		val filterDstream: DStream[String] = logDstream.transform(rdd => {
			val blackList: List[(Long, Boolean)] = List((1L, true), (2L, true), (3L, true))
			val blackListRDD: RDD[(Long, Boolean)] = spark.sparkContext.parallelize(blackList)

			val userid_log_rdd: RDD[(Long, String)] = rdd.map(log => {
				val strs: Array[String] = log.split(",")
				(strs(3).toLong, log)
			})

			/**
				* (Long, (String, Option[Boolean]))
				* (k,(k,v))
				* K  Long : user_id
				* K  String : log
				* V  Option
				*/
			val joinRDD: RDD[(Long, (String, Option[Boolean]))] = userid_log_rdd.leftOuterJoin(blackListRDD)
			val filterRDD: RDD[(Long, (String, Option[Boolean]))] = joinRDD.filter(tuple => {
				tuple._2._2.isEmpty
			})
			filterRDD.map(tuple => tuple._2._1)
		})
		filterDstream
	}

	/** 制定第四步黑名单规则
		* 动态生成黑名单
		*
		* 传入过滤好的数据logDstream  ， 将结果持久化，因此调用参数 spark:SparkSession
		* */
	def generaterDynamicBlackList(filterDstream: DStream[String], spark: SparkSession): Unit = {

		val date_userid_advid_dsm: DStream[(String, Long)] = filterDstream.map(log => {
			val logs: Array[String] = log.split(",")
			val date: String = DateUtils.formatDateKey(new Date(log(0).toLong))
			val userid: Long = log(3).toLong
			val advid: Long = log(4).toLong
			(date + " " + userid + " " + advid, 1L)
		})
		val date_userid_advid_dsm_count: DStream[(String, Long)] = date_userid_advid_dsm.reduceByKey(_ + _) //updateByKey
		date_userid_advid_dsm_count.foreachRDD(rdd => {

			rdd.foreachPartition(p => {
				val con: Connection = MysqlPool.getJdbcCoon()
				p.foreach(log => {
					val sql =
						"""
					INSERT INTO tmp_(now(),userid,advid,count)
					"""
				})
				MysqlPool.releaseConn(con)
			})
		})

		val df: DataFrame = spark.read.format("jdbc")
			.option("url", ConfigurationManager.getProperty(Constants.JDBC_URL))
			.option("username", ConfigurationManager.getProperty(Constants.JDBC_USER))
			.option("password", ConfigurationManager.getProperty(Constants.JDBC_PASSWORD))
			.option("dbtable", "tem_click_count")
			.load()
		df.createOrReplaceTempView("tmp_click_count_f")

		val sql =
			"""
			SELECT
				userid
	    FROM
		 (
	     SELECT
          SUM(click_count) count,time,userid,advid
       FROM
          tmp_click_count_f
	     GROUP BY
			    time.useid,advid
		 ) t
	   WHERE
				t.count >= 100
			"""
		val black_list: Dataset[Row] = spark.sql(sql).distinct() //去重

		/**
			* 将结果持久化到mysql或者Redis中
			*
			*/
		black_list.write.mode(SaveMode.Append).jdbc("url", "black_list", new Properties())
	}

	/**
		*
		* 第五步：
		* 统计每天各省份各城市的广告点击量
		*
		*/

	def calculatorCityClickCount(filterDstream: DStream[String]): DStream[(String, Long)] = {
		val date_province_city_advid: DStream[(String, Long)] = filterDstream.map(log => {
			val logs: Array[String] = log.split(",")
			val date: String = DateUtils.formatDateKey(new Date(logs(0).toLong))
			val province: Long = logs(1).toLong
			val city: Long = logs(2).toLong
			val advid: Long = logs(4).toLong
			(date + " " + province + " " + city + " " + advid, 1L)
		})
		val result: DStream[(String, Long)] = date_province_city_advid.updateStateByKey((input: Seq[Long], values: Option[Long]) => {
			val currentCount: Long = input.sum
			val lastCount: Long = values.getOrElse(0).toLong
			Some(currentCount + lastCount)
		})
		result
	}

	/**
		*
		* 第六步：
		* 统计各区域热门商品的点击次数  top N
		*
		*/

	def calculatorProvinceAdbClickCountTopN(cityClickCount: DStream[(String, Long)], spark: SparkSession): Unit = {
		cityClickCount.transform(rdd => {
			val rowRDD: RDD[Row] = rdd.map(tuple => {
				val dpca: String = tuple._1
				val count: Long = tuple._2
				val dpcas = dpca.split("_")
				(dpcas(0) + " " + dpcas(1) + " " + dpcas(3).toLong, count)
			}).reduceByKey((_ + _))
				.map(tuple => {
					val dpa: String = tuple._1
					val dpas: Array[String] = dpa.split("_")
					val count: Long = tuple._2
					Row(dpas(0), dpas(1).toLong, dpas(2).toLong, count)
				})
			rowRDD

			val schame: StructType = StructType(
				StructField("date", StringType, true) ::
					StructField("provinceid", LongType, true) ::
					StructField("advid", LongType, true) ::
					StructField("count", LongType, true) :: Nil
			)
			val df: DataFrame = spark.createDataFrame(rowRDD, schame)
			df.createOrReplaceTempView("tmp_date_province_advid_count")
			val sql =
				"""
			 select
 		        date,provinceid,advid,count
					(select
		        date,provinceid,advid,count,row_number() over (partition by provinceid order by count desc) rank
					from
					 tmp_date_province_advid_count
					 )   tmp
					  where
			         tmp <= N
				"""
			spark.sql(sql)
			null
		})
	}
}