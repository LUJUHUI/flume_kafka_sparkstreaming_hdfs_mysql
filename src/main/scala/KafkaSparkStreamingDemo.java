import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;


public class KafkaSparkStreamingDemo {
	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setAppName("kafkaSpark").setMaster("local[*]");
		JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Seconds.apply(5));

		Map<String, Object> kafkaParams = new HashMap<>();
		/*kafka的端口号*/
		kafkaParams.put("bootstrap.servers", "manager:9092,namenode:9092,datanode:9092");
		/*k-v的反序列化*/
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		/*kafka的组号*/
		kafkaParams.put("group.id", "kafka_spark");
		/*偏移量重置*/
		kafkaParams.put("auto.offset.reset", "latest");
		/*是否自动提交*/
		kafkaParams.put("enable.auto.commit", false);

		/*kafka的已经创建的主题*/
		Collection<String> topics = Arrays.asList("realtime");

		/*创建一个离散流*/
		JavaInputDStream<ConsumerRecord<String, String>> stream =
				KafkaUtils.createDirectStream(
						streamingContext,
						LocationStrategies.PreferConsistent(),
						ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
				);

		/*对kafka消费端取出来的数据进行扁平化处理（string，string）-> string*/
		JavaDStream<String> wordsDS = stream.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, String>() {
			@Override
			public Iterator<String> call(ConsumerRecord<String, String> r) throws Exception {
				String value = r.value();
				List<String> list = new ArrayList<>();
				String[] arr = value.split(" ");
				for (String s : arr) {
					list.add(s);
				}
				return list.iterator();
			}
		});

		/*映射成元组*/
		JavaPairDStream<String, Integer> pairDS = wordsDS.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) throws Exception {
				return new Tuple2<>(s, 1);     //s代表单词，1代表单词出现的次数
			}
		});

		/*聚合*/
		JavaPairDStream<String, Integer> countDS = pairDS.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});

		/*打印*/
		countDS.print();

		streamingContext.start();
		streamingContext.awaitTermination();

	}
}
