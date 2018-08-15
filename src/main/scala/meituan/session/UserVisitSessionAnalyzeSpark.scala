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

    val sessionid_rows: RDD[(Long, Iterable[Row])] = filter_rdd.groupBy(row => row(2).toString.toLong)

    sessionid_rows.filter(tuple => {
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
      * 第五步 [获取点击，下单，支付排名前十的品类]
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

  }


}

