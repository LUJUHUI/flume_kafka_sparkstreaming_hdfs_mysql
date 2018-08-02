package realtime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @ author: create by LuJuHui
 * @ date:2018/8/1
 */
public class MapperM extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] split = value.toString().split("\t");
		for (String word : split) {
			context.write(new Text(word), new IntWritable(1));
		}
	}
}

class ReducerR extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable count : values) {
			sum += count.get();
		}
		context.write(key, new IntWritable(sum));
	}
}

class DriverD {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("dfs.defaultFS", "hdfs://namenode:8020");
		System.setProperty("HADOOP_USER_NAME", "namenode");

		Job job = Job.getInstance(conf);

		job.setJarByClass(DriverD.class);

		job.setMapperClass(MapperM.class);
		job.setReducerClass(ReducerR.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		Path inputpath = new Path("/user/kafka/data/part-00000");
		Path outputpath = new Path("/user/kafka/out");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputpath)) {
			fs.delete(outputpath, true);
		}

		FileInputFormat.addInputPath(job, inputpath);
		FileOutputFormat.setOutputPath(job, outputpath);

		boolean b = job.waitForCompletion(true);

		System.exit(b ? 0 : 1);

	}
}