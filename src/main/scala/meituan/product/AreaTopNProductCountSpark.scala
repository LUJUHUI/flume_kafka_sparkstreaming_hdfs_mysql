package meituan.product

import java.lang

import com.alibaba.fastjson.JSONObject
import meituan.conf.ConfigurationManager
import meituan.constant.Constants
import meituan.dao.factory.DAOFactory
import meituan.domain.Task
import meituan.utils.ParamUtils
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
	* Created by Administrator on 2017/6/27.
	*/
object AreaTopNProductCountSpark {
	def main(args: Array[String]): Unit = {
		/**
			* 第一步：创建sparksession
			*/
		val spark = SparkSession.builder()
			.appName(Constants.SPARK_APP_NAME_PRODUCT)
			.config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse") //在hive中读数据
			.enableHiveSupport()
			.getOrCreate()


		spark.udf.register("concast_long_string", (city_id: Long, city_name: String, delimiter: String) => {
			city_id + delimiter + city_name
		})

		spark.udf.register("groupdinstinct", GroupDistinctUDAF)
		/**
			* 第二步：获取参数
			* 只传入时间
			*/
		val taskID: lang.Long = ParamUtils.getTaskIdFromArgs(args)
		val taskDAO = DAOFactory.getTaskDAO
		val task: Task = taskDAO.findById(taskID)
		val taskJson: JSONObject = ParamUtils.getTaskParam(task)
		val start_date = ParamUtils.getParam(taskJson, Constants.PARAM_START_DATE)
		val end_date: String = ParamUtils.getParam(taskJson, Constants.PARAM_END_DATE)
		/**
			* 第三步：根据日期获取用户行为数据
			*/
		val actionRDD: RDD[(Long, Row)] = getActionByDateRange(spark, start_date, end_date)
		//true放回   不放回 ：伯努利采样  第二个参数：这个元素出现的概率  取值 只能是0-1  最后的个数其实是不定的
		//泊松采样
		//第二个参数指的是 出现平均期望次数
		// actionRDD.sample(true,)
		/**
			* 第四步：获取城市信息表  Mysql
			*/
		val cityid2cityInfoRDD: RDD[(Long, Row)] = getCityId2CityInfoRDD(spark)

		/**
			* 第五步：第三步和第四步的数据进行join 转换DataFrame
			*/
		generateTempBasicTable(spark, actionRDD, cityid2cityInfoRDD)

		/**
			* 第六步：统计各区域热门商品次数 （特殊要求）                    rownumber() over()
			*/
		generateTempAreaProductCount(spark)

		/**
			* 第七步：上一步统计出来的结果 跟  商品信息表进行join
			*/
		genarteTempAreaFullProductCountTable(spark)

		/**
			* 第八步：  topN
			*/
		getTop10(spark)

		/**
			* 第九步  把结果进行持久化
			* 我们作为大数据开发的人员，做到这儿一般就可以了。
			* 后面至于JavaEE的同事怎么展现
			*/
		persistResult(spark)

	}

	def getActionByDateRange(spark: SparkSession, start_date: String, end_date: String): RDD[(Long, Row)] = {
		/**
			* NULL
			* 'NULL'  'null'  '’
			*/
		val sql =
			s"""
       SELECT
          city_id,click_product_id
       FROM
          user_visit_action
       WHERE
         click_product_id is not null
         and click_product_id != 'NULL'
         and click_product_id != 'null'
         and click_product_id != ''
         and date >= ${start_date}
         and date <= ${end_date}
      """
		spark.sql(sql).map(row => (row.getLong(0), row)).rdd

	}

	/**
		* 从MySQL里面获取城市信息
		*
		* @param spark
		* @return RDD[(Long, Row)] 其中long代表的是cityID，Row代表的是cityID对应的每一行数据
		*/
	def getCityId2CityInfoRDD(spark: SparkSession): RDD[(Long, Row)] = {
		val df: DataFrame = spark.read.format("jdbc")
			.option("url", ConfigurationManager.getProperty(Constants.JDBC_URL))
			.option("username", ConfigurationManager.getProperty(Constants.JDBC_USER))
			.option("password", ConfigurationManager.getProperty(Constants.JDBC_PASSWORD))
			.option("table", "city_info")
			.load()
		df.rdd.map(row => (row.getLong(0), row)) // row.getLong(0) 其中0代表cityID
	}

	/**
		*
		* @param spark
		* @param actionRDD          用户行为数据  cityid,productid
		* @param cityid2cityInfoRDD cityid,row
		*/
	def generateTempBasicTable(spark: SparkSession, actionRDD: RDD[(Long, Row)],
	                           cityid2cityInfoRDD: RDD[(Long, Row)]): Unit = {
		//(Long,     (Row,     Row))
		//cityid,   Row（包含cityid,productid）    Row
		val joinRDD: RDD[(Long, (Row, Row))] = actionRDD.join(cityid2cityInfoRDD)
		val rowRDD = joinRDD.map(tuple => {
			val city_id = tuple._1
			val user_row = tuple._2._1
			val city_row = tuple._2._2
			val click_product_id = user_row.getLong(1)
			val city_name = city_row.getString(1)
			val city_area = city_row.getString(2)

			//cityid  click_product_id  city_name   city_area
			Row(city_id, click_product_id, city_name, city_area)
		})
		val schema = StructType(
			StructField("city_id", LongType, true) ::
				StructField("click_product_id", LongType, true) ::
				StructField("city_name", StringType, true) ::
				StructField("area", StringType, true) :: Nil
		)
		val df = spark.createDataFrame(rowRDD, schema)
		df.createOrReplaceTempView("action_city") //创建视图表action_city

	}

	/**
		* 求各区域热门商品
		* action_city
		* city_id
		* click_product_id
		* city_name
		* area
		* 华北地区：A，B，C，D四个城市  华北：A B C    A B           top2
		*
		* city_id,city_name    city_id:cityname    udf  abc
		* 1 beijing             1:beijing
		* 2 shanghai            2:shanghai
		* 3 shenzheng          3:shengzheng
		* select   abc(city_id,city_name) from action_city
		* 1:beijing
		* 1:beijing
		* 2:shanghai                 ->  1:beijing,2:shagnhai,3:shenzheng      UDAF
		* 2:shanghai
		* 3:shenzheng
		* 3:shenzheng
		*
		* @param spark
		*/
	def generateTempAreaProductCount(spark: SparkSession): Unit = {
		/** groupdinstinct 的作用是对查询的结果进行去重 */
		val sql =
			"""
       SELECT
           area,click_product_id,count(*) click_count,
           groupdinstinct(concast_long_string(city_id,city_name,':')) city_infos
       FROM
           action_city
       GROUP BY
           area,click_product_id

      """
		val df = spark.sql(sql)

		df.createOrReplaceTempView("temp_area_product_click_count")      //创建视图表 temp_area_product_click_count


	}

	/**
		* 与商品信息表进行join 得到  完整信息表
		*
		* @param saprk
		*/
	def genarteTempAreaFullProductCountTable(saprk: SparkSession): Unit = {
		/**
			* product_status:0  我们需要展示的是：自营，要么第三方
			* json
			*/
		val sql =
			"""
         SELECT
            tcc.area,
            tcc.click_product_id,
            tcc.click_count,
            tcc.city_infos,
            pi.product_name,
            //预留一小点 product_status
            FROM
             temp_area_product_click_count tcc
            JOIN
             product_info pi
             ON
             tcc.click_product_id=pi.product_id
      """
		val df = saprk.sql(sql)
		df.createOrReplaceTempView("area_product_count")

	}

	def getTop10(spark: SparkSession): Unit = {

		val sql =
			"""
         SELECT
          CASE
            WHEN area='华北' or area='华东'     THEN 'A级'
            WHEN area='华南' or area='华中'     THEN 'B级'
            WHEN area='西北' or area='西南'     THEN  'C级'
            WHEN area='东北' THEN 'D级'
            ELSE 'E级'
          END  area_level,
          click_product_id,
          click_count,
          city_infos,
          product_name,
          product_status
         FROM
       (
          SELECT
             area,click_product_id,click_count,city_infos,product_name,product_status
          ROW_NUMBER() OVER(PARTITION BY area ORDER BY click_count DESC) rank
          FROM
         area_product_count
         ) tmp
         WHERE temp.rank <= N

      """
		val df = spark.sql(sql)

		df.createOrReplaceTempView("result")


	}

	/**
		* 持久化 结果
		*
		* @param spark
		*/
	def persistResult(spark: SparkSession): Unit = {
		val df = spark.read.table("result")
		df.rdd.foreachPartition(p => {
			//从连接池里面创建一个连接
			p.foreach(result => {
				//插入数据
			})
			//把连接还到连接池
		})
	}


}
