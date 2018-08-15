package meituan.constant;

/**
 * @ author: create by LuJuHui
 * @ date:2018/8/13
 */
public interface Constants {

	/**
	 * Project Configuration Constants
	 */
	String JDBC_DRIVER = "jdbc.driver";//public static final
	String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
	String JDBC_URL = "jdbc.url";
	String JDBC_USER = "jdbc.user";
	String JDBC_PASSWORD = "jdbc.password";

	String SPARK_SQL_JDBC_URL = "spark.sql.jdbc.url";
	String SPARK_SQL_JDBC_URL_PROD = "spark.sql.jdbc.url.prod";

	String SPARK_LOCAL = "spark.local" ;

	String KAFKA_METADATA_BROKER_LIST = "metadata.broker.list";
	String KAFKA_TOPICS = "kafka.topics";


	/**
	 * Spark Application Constants
	 */
	String SPARK_APP_NAME_SESSION = "UserVisitSessionAnalyzeSpark" ;
	String SPARK_APP_NAME_PRODUCT = "AreaTop3ProductSpark" ;
	String SPARK_APP_NAME_ADV="AdvtotalCountSpark";

	String FIELD_SESSION_ID = "sessionid";
	String FIELD_SEARCH_KEYWORDS = "searchKeywords";
	String FIELD_CLICK_CATEGORY_IDS = "clickCategoryIds";
	String FIELD_AGE = "age";
	String FIELD_PROFESSIONAL = "professional";
	String FIELD_CITY = "city";
	String FIELD_SEX = "sex";


	String FIELD_CATEGORY_ID = "categoryId" ;
	String FIELD_CLICK_COUNT = "clickCount" ;
	String FIELD_ORDER_COUNT = "orderCount" ;
	String FIELD_PAY_COUNT = "payCount" ;

	String SESSION_COUNT = "session_count";

	String TIME_PERIOD_1s_4s = "1s_4s";
	String TIME_PERIOD_4s_7s = "4s_7s";
	String TIME_PERIOD_7s_10s = "7s_10s";
	String TIME_PERIOD_10s_30s = "10s_30s";
	String TIME_PERIOD_30s_60s = "30s_60s";
	String TIME_PERIOD_1m_3m = "1m_3m";
	String TIME_PERIOD_3m_10m = "3m_10m";
	String TIME_PERIOD_10m_30m = "10m_30m";
	String TIME_PERIOD_30m = "30m";

	String STEP_PERIOD_1_3 = "1_3";
	String STEP_PERIOD_4_6 = "4_6";
	String STEP_PERIOD_7_9 = "7_9";
	String STEP_PERIOD_10_29 = "10_29";
	String STEP_PERIOD_30_59 = "30_59";
	String STEP_PERIOD_60 = "60";

	/**
	 * Task Constants
	 */
	String PARAM_START_DATE = "startDate";
	String PARAM_END_DATE = "endDate";
	String PARAM_START_AGE = "startAge";
	String PARAM_END_AGE = "endAge";
	String PARAM_PROFESSIONALS = "professionals";
	String PARAM_CITIES = "cities";
	String PARAM_SEX = "sex";
	String PARAM_KEYWORDS = "keywords";
	String PARAM_CATEGORY_IDS = "categoryIds";
	String FIELD_VISIT_LENGTH = "visitLength";
	String FIELD_STEP_LENGTH = "stepLength";
	String FIELD_START_TIME = "startTime" ;
}

