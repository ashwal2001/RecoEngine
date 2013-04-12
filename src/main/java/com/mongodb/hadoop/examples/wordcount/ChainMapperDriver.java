package com.mongodb.hadoop.examples.wordcount;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;

public class ChainMapperDriver {
	public static void main(String[] args) throws Exception {

		JobConf jobConf = new JobConf(ChainMapperDriver.class);

		jobConf.setOutputKeyClass(IntWritable.class);
		jobConf.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

		ChainMapper.addMapper(jobConf, ChainMapperOne.class,
				LongWritable.class, Text.class, IntWritable.class, Text.class,
				false, jobConf);
		ChainMapper.addMapper(jobConf, ChainMapperTwo.class, IntWritable.class,
				Text.class, LongWritable.class, Text.class, false, jobConf);
		jobConf.setReducerClass(IdentityReducer.class);

		JobClient jobClient = new JobClient();
		jobClient.setConf(jobConf);

		// jobClient.runJob(jobConf);
		JobClient.runJob(jobConf);

	}

	public static class ChainMapperOne extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, Text> {

		@Override
		public void map(LongWritable key, Text value, OutputCollector output,
				Reporter reporter) throws IOException {

			String[] inputFields = value.toString().split("\t");
			int intValue = Integer.parseInt(inputFields[0]);
			System.out.println("input value in ChainMapperOne >> " + intValue);

			output.collect(new IntWritable(intValue), new Text(inputFields[1]));
		}

	}

	public class ChainMapperTwo extends MapReduceBase implements
			Mapper<IntWritable, Text, LongWritable, Text> {

		@Override
		public void map(IntWritable key, Text value, OutputCollector output,
				Reporter reporter) throws IOException {

			int intVal = key.get();
			System.out.println("input value in ChainMapperTwo >> " + intVal);

			output.collect(new LongWritable(new Long(intVal * intVal)), value);
		}

	}

}
