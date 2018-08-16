package meituan.session

/**
	* @ author: create by LuJuHui
	* @ date:2018/8/13
	*/

import meituan.constant.Constants
import meituan.dao.factory.DAOFactory
import meituan.utils.ParamUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer


object UserVisitSessionAnalyzeSpark {
	def main(args: Array[String]): Unit = {

		/**
			* 第一步 创建sparksession
			**/
		val spark = SparkSession.builder()
			.appName(Constants.SPARK_APP_NAME_SESSION) //不能用硬编码
			// .master("local[2]")
			.config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse")
			.enableHiveSupport() //支持hive
			.getOrCreate(); //如果有这个目录就获取，没有就直接创建

		/**
			* 第二步获取参数
			*/
		//如果想要数据库的数据，先要获取到taskID号，然后调用工厂类接口taskDao
		val taskID = ParamUtils.getTaskIdFromArgs(args)
		val taskDAO = DAOFactory.getTaskDAO //通过接口，调用数据库
		//根据taskID，去数据库中查找数据，获取到task对象
		val task = taskDAO.findById(taskID)
		//jsonOBJECT
		val taskParam = ParamUtils.getTaskParam(task) //传进一个task对象(已经从数据库中获取了数据)，将其封装成json格式的对象taskParam
		val start_time = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE) //根据传入的json对象及需要获取的字段名，即可获得相应参数的value数据
		val end_time = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)


		/**
			* 根据时间范围从hive里面获取数据
			*
			* @param spark     sparksession
			* @param startTime 页面传进来的开始时间
			* @param endTime   页面传进来的结束时间
			* @return
			*/
		def getUserActionDataByDateRange(spark: SparkSession, startTime: String, endTime: String): RDD[Row] = {
			val sql =
				s"""
       SELECT
          *
       FROM
          user_visit_action
       WHERE
          date >= ${startTime} and date <= ${endTime}
      """
			val df: DataFrame = spark.sql(sql)
			df.rdd
		}


		/**
			* 第三步  根据日期获取hive里面的数据
			*/
		val row_rdd_useraction = getUserActionDataByDateRange(spark, start_time, end_time)
		/**
			* 过滤
			* 需要过滤的条件：
			* 1） 根据时间范围
			* startdate
			* 起始时间
			* enddate
			* 结束时间
			* 2）性别
			* 男/女
			* 3）年龄范围
			* 4）职业
			* 多选的
			* 只需要我们能满足其中的一个就可以了
			* 5）城市
			* 多选
			* 6） 搜索词
			* 多选的   【小龙虾，皮皮虾】
			* 7）
			* 点击品类
			* 可以多选
			*/
		val filter_rdd = row_rdd_useraction

		/**
			* val STEP_PERIOD_1_3: String = "1_3"
			* val STEP_PERIOD_4_6: String = "4_6"
			* val STEP_PERIOD_7_9: String = "7_9"
			* val STEP_PERIOD_10_29: String = "10_29"
			* val STEP_PERIOD_30_59: String = "30_59"
			* val STEP_PERIOD_60: String = "60"
			* 第四步 会话的步长比例
			*/
		//filter_rdd.groupBy(回话id)  聚合
		val s_1_3 = spark.sparkContext.longAccumulator(Constants.STEP_PERIOD_1_3)
		val s_4_6 = spark.sparkContext.longAccumulator(Constants.STEP_PERIOD_4_6)
		val s_7_9 = spark.sparkContext.longAccumulator(Constants.STEP_PERIOD_7_9)
		val s_10_29 = spark.sparkContext.longAccumulator(Constants.STEP_PERIOD_10_29)
		val s_30_59 = spark.sparkContext.longAccumulator(Constants.STEP_PERIOD_30_59)
		val s_60 = spark.sparkContext.longAccumulator(Constants.STEP_PERIOD_60)

		val sessionid_rows_rdd: RDD[(Long, Iterable[Row])] = filter_rdd.groupBy(row => row(2).toString.toLong)

		sessionid_rows_rdd.filter(tuple => {
			val rows: Iterable[Row] = tuple._2
			val step = rows.size
			if (step >= 1 && step <= 3) {
				s_1_3.add(1)
			} else if (step >= 4 && step <= 6) {
				s_4_6.add(1)
			} else if (step >= 7 && step <= 9) {
				s_7_9.add(1)
			} else if (step >= 10 && step <= 29) {
				s_10_29.add(1)
			} else if (step >= 30 && step <= 59) {
				s_30_59.add(1)
			} else if (step >= 60) {
				s_60.add(1)
			}
			true
		})


		/**
			* 第五步 [获取点击，下单，支付排名前10的品类]
			* 要掌握
			* 1） 首先要获取到所有的品类，并且要去重
			* 点击的品类，下单的品类， 支付的品类
			* allcategoryid
			* 2）分别对点击的品类，下单的品类，支付的品类   进行单词计数运算
			* 11  23 11 23
			* 11 2
			* 23 2
			*
			* 3)
			*
			* allcategoryid[categoryid,categoryid].join click_categoryid_count[categoryid,count]
			* .join(order_categoryid_count)
			* .join(pay_categoryid_count)
			* 二次排序
			*/
		/*5.1 求出所有的品类ID    k:categoryid  v: categoryid*/
		val allCategoryidRDD: RDD[(Long, Long)] = getAllCategoryId(sessionid_rows_rdd).distinct()

		def getAllCategoryId(sessionid_rows_rdd: RDD[(Long, Iterable[Row])]): RDD[(Long, Long)] = {
			val list: ListBuffer[Tuple2[Long, Long]] = new ListBuffer[Tuple2[Long, Long]]
			val listRDD = sessionid_rows_rdd.flatMap(tuple => {
				val rows: Iterator[Row] = tuple._2.iterator
				while (rows.hasNext) {
					val row = rows.next()
					val clickCategoryId = row.getLong(7)
					val orderCategoryIdStr = row.getString(9)
					val payCategoryIdStr = row.getString(11)
					if (clickCategoryId != null) {
						list += Tuple2(clickCategoryId, clickCategoryId)
					}

					if (orderCategoryIdStr != null) {
						val orderCategoryStrs = orderCategoryIdStr.split(",")
						for (orderCategoryid <- orderCategoryStrs) {
							list += Tuple2(orderCategoryid.toLong, orderCategoryid.toLong)
						}
					}

					if (payCategoryIdStr != null) {
						val payCategoryStrs = payCategoryIdStr.split(",")
						for (payCategoryid <- payCategoryStrs) {
							list += Tuple2(payCategoryid.toLong, payCategoryid.toLong)
						}
					}
				}
				list
			})
			listRDD
		}

		/**5.2 分别求出点击的、下单的、支付的品类，并求出次数   k:categoryid v:count*/

		/*===================5.1.1 获取点击品类的次数==========begin==========================================*/
		val clickCategoryid2CountRDD: RDD[(Long, Long)] = getclickCategoryid2CountRDD(sessionid_rows_rdd)

		def getclickCategoryid2CountRDD(sessionid_rows_rdd: RDD[(Long, Iterable[Row])]): RDD[(Long, Long)] = {
			sessionid_rows_rdd.flatMap(tuple => {
				val session_id = tuple._1
				val rows: Iterator[Row] = tuple._2.iterator
				val list: ListBuffer[Tuple2[Long, Row]] = new ListBuffer[(Long, Row)]
				while (rows.hasNext) {
					val row: Row = rows.next()
					list += Tuple2(session_id.toLong, row)
				}
				list
			}).filter(tuple => {
				val session_id = tuple._1
				val row = tuple._2
				if (row.getLong(7) != null) true else false
			}).map(tuple => {
				val row: Row = tuple._2 //tuple的形式为tuple[(Long,ROW)]=[session_id,(date,user_id,....,pay_product_id)]
				(row.getLong(7), 1L) //偏移量为7，对应ROW中的字段为click_catagory_id
			}).reduceByKey((_ + _)) //按key值进行聚合排序
		}
		/*===================5.1.1 获取点击品类的次数==========end==========================================*/

		/*===================5.1.2 获取下单品类的次数===========begin=======================================*/
		/**
		* 思路：
		* 定义一个getorderCategoryid2CountRDD函数，
		* 其参数为清洗之后的sessionid_rows_rdd，
		* 参数类型为 RDD[(Long, Iterable[Row])])，
		* 返回类型为 RDD[(Long, Long)]
		*
		* */
		val orderCategoryid2CountRDD: RDD[(Long, Long)] = getorderCategoryid2CountRDD(sessionid_rows_rdd)

		def getorderCategoryid2CountRDD(sessionid_rows_rdd: RDD[(Long, Iterable[Row])]): RDD[(Long, Long)] = {
			//对RDD进行扁平化处理
			sessionid_rows_rdd.flatMap(tuple => {
				val session_id = tuple._1 //其中tuple._1为session_id
				val rows: Iterator[Row] = tuple._2.iterator //tuple._2为对应一行表字段下的数据，对其进行迭代
				val list: ListBuffer[Tuple2[Long, Row]] = new ListBuffer[(Long, Row)] //新建一个ListBuffer[(Long, Row)]
				while (rows.hasNext) {
					val row: Row = rows.next()
					list += Tuple2(session_id.toLong, row) //将迭代的每一条row值增加到list之中
				}
				list //返回list
			}).filter(tuple => { //对返回的list进行筛选 list[(long,ROW)]
				val row: Row = tuple._2 //将ROW赋值给row，其中tuple的形式为tuple[(Long,ROW)]=[session_id,(date,user_id,....,pay_product_id)]
			val rowStr: String = row.getString(9) //偏移量为9，对应ROW中的字段为order_catagory_id
				if (row.getString(9) != null) true else false //判断字段order_catagory_id是否为空
			}).flatMap(tuple => { //对返回的list进行扁平化处理
				val row: Row = tuple._2
				val orderStr: String = row.getString(9) //偏移量为9，对应ROW中的字段为order_catagory_id
				val orderStrs: Array[String] = orderStr.split(",")
				val list: ListBuffer[(Long, Long)] = new ListBuffer[(Long, Long)]
				for (orderCategoryId <- orderStrs) {
					list += Tuple2(orderCategoryId.toLong, 1L)
				}
				list
			}).reduceByKey((_ + _))
		}
		/*===================5.1.2 获取下单品类的次数===========end=======================================*/

		/*===================5.1.3 获取支付品类的次数===========begin=====================================*/
		val payCategoryid2CountRDD: RDD[(Long, Long)] = getpayCategoryid2CountRDD(sessionid_rows_rdd)

		def getpayCategoryid2CountRDD(sessionid_rows_rdd: RDD[(Long, Iterable[Row])]): RDD[(Long, Long)] = {
			sessionid_rows_rdd.flatMap(tuple => {
				val session_id: Long = tuple._1
				val rows: Iterator[Row] = tuple._2.iterator
				val list: ListBuffer[Tuple2[Long, Row]] = new ListBuffer[(Long, Row)]
				while (rows.hasNext) {
					val row: Row = rows.next()
					list += Tuple2(session_id.toLong, row)
				}
				list
			}).filter(tuple => {
				val row: Row = tuple._2
				val payidStr: String = row.getString(11)
				if (row.getString(11) != null) true else false
			}).flatMap(tuple => {
				val row: Row = tuple._2
				val payStr: String = row.getString(11)
				val payStrs: Array[String] = payStr.split(",")
				val list: ListBuffer[Tuple2[Long, Long]] = new ListBuffer[(Long, Long)]
				for (payCategoryId <- payStrs) {
					list += Tuple2(payCategoryId.toLong, 1L)
				}
				list
			}).reduceByKey((_ + _))
		}
		/*===================5.1.3 获取支付品类的次数===========end=====================================*/

		/**
			* @ Date: 2018/8/16
			* @ Param: allCategoryidRDD   总的品类
			* @ Param: clickCategoryid2CountRDD   点击品类出现的次数
			* @ Param: orderCategoryid2CountRDD   下单品类出现的次数
			* @ Param: payCategoryid2CountRDD     支付品类出现的次数
			*/

		val joinCategoryandData: RDD[(Long, String)] = joinCategoryAndData(allCategoryidRDD,clickCategoryid2CountRDD,orderCategoryid2CountRDD,payCategoryid2CountRDD)
		def joinCategoryAndData(allCategoryidRDD: RDD[(Long, Long)],
		                        clickCategoryid2CountRDD: RDD[(Long, Long)],
		                        orderCategoryid2CountRDD: RDD[(Long, Long)],
		                        payCategoryid2CountRDD: RDD[(Long, Long)]): RDD[(Long, String)] = {
			/**
			* (categoryid,categoryid) leftjoin (clickCategoryid,count)
			* (Long,     (Long, Option[Long]))  是一个tuple元组  其中Option是为了避免使用null或空值
			* (categoryid,   (categoryid, count))  如果count为空，则返回long
			*
			* 点击
			* */
			val tempJoinRDD: RDD[(Long, (Long, Option[Long]))] = allCategoryidRDD.leftOuterJoin(clickCategoryid2CountRDD)
			val tmpRDD1: RDD[(Long, String)] = tempJoinRDD.map(tuple => {
				val categoryID: Long = tuple._1
				val clickCategoryidCount: Long = tuple._2._2.getOrElse(0)
				val value = Constants.FIELD_CATEGORY_ID + "=" + categoryID + " | " + Constants.FIELD_CLICK_COUNT + "=" + clickCategoryidCount
				(categoryID, value)
			})

			/**
			* (Long, (String, Option[Long]))
			* categoryid, (value,ordercount)  此处的value是上一个返回值的value
			*
			* 下单
			* */
			val tempJoinRDD1: RDD[(Long, (String, Option[Long]))] = tmpRDD1.leftOuterJoin(orderCategoryid2CountRDD)
			val tmpRDD2 = tempJoinRDD1.map(tuple => {
				val categoryID: Long = tuple._1
				var value = tuple._2._1
				val orderCategoryidCount: Long = tuple._2._2.getOrElse(0)
				value = value + " | " + Constants.FIELD_ORDER_COUNT + "=" + orderCategoryidCount
				(categoryID, value)
			})
			/**
				* 支付
				* */
			val tempJoinRDD2: RDD[(Long, (String, Option[Long]))] = tmpRDD2.leftOuterJoin(payCategoryid2CountRDD)
			val tmpRDD3: RDD[(Long, String)] = tempJoinRDD2.map(tuple => {
				val categoryID: Long = tuple._1
				var value: String = tuple._2._1
				val payCategoryidCount: Long = tuple._2._2.getOrElse(0)
				value = value + " | " + Constants.FIELD_PAY_COUNT + "=" + payCategoryidCount
				(categoryID, value)
			})
			tmpRDD3
		}


	}
}

