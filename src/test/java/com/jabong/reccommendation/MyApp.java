package com.jabong.reccommendation;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyApp extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// Configuration processed by ToolRunner
		Configuration conf = getConf();

		// Create a JobConf using the processed conf
		JobConf job = new JobConf(conf, MyApp.class);

		// Process custom command-line options
		Path in = new Path(args[1]);
		Path out = new Path(args[2]);

		// Specify various job-specific parameters
		job.setJobName("my-app");
//		job.setInputPath(in);
//		job.setOutputPath(out);
//		job.setMapperClass(TokenCounterMapper.class);
//		job.setReducerClass(MyReducer.class);

		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run(new Configuration(), new MyApp(), args);

		System.exit(res);
	}

	public static class TokenCounterMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer<Key> extends
			Reducer<Key, IntWritable, Key, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Key key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
}