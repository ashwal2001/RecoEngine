package com.mongodb.hadoop.examples.wordcount;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapred.lib.IdentityReducer;

public class JobDep {

	public static class SortMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, DoubleWritable, DoubleWritable> {

		@SuppressWarnings("unchecked")
		@Override
		public void map(LongWritable key, Text value,
				@SuppressWarnings("rawtypes") OutputCollector output,
				Reporter reporter) throws IOException {
			String[] tokens = value.toString().split(",");
			double dvalue;

			for (String token : tokens) {
				dvalue = Double.parseDouble(token);
				System.out.println("dvalue: " + dvalue);
				output.collect(new DoubleWritable(dvalue), new DoubleWritable(
						dvalue));
			}
		}

	}

	public static class WordSearchMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {
		static String keyword;

		@Override
		public void configure(JobConf jobConf) {
			keyword = jobConf.get("keyword");
			System.out.println("search keyword>> " + keyword);
			super.configure(jobConf);
		}

		@Override
		public void map(LongWritable key, Text value, OutputCollector output,
				Reporter reporter) throws IOException {
			int wordPos;
			System.out.println("value.toString()>> " + value.toString());
			if (value.toString().contains(keyword)) {
				wordPos = value.find(keyword);
				output.collect(value, new IntWritable(wordPos));
			}
		}

	}

	public static void main(String[] args) throws Exception {

		JobConf sortConf = new JobConf(JobDep.class);

		sortConf.setJobName("SortJob");

		sortConf.setMapperClass(SortMapper.class);
		sortConf.setReducerClass(IdentityReducer.class);

		sortConf.setOutputKeyClass(DoubleWritable.class);
		sortConf.setOutputValueClass(DoubleWritable.class);

		String out = args[0] + System.nanoTime();
		FileInputFormat.setInputPaths(sortConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(sortConf, new Path(out));

		JobConf searchConf = new JobConf(JobDep.class);
		searchConf.setJobName("SearchJob");

		searchConf.setMapperClass(WordSearchMapper.class);
		searchConf.setReducerClass(IdentityReducer.class);

		searchConf.setOutputKeyClass(Text.class);
		searchConf.setOutputValueClass(IntWritable.class);

		FileInputFormat
				.setInputPaths(searchConf, new Path(out + "/part-00000"));
		FileOutputFormat.setOutputPath(searchConf, new Path(args[1]));

		searchConf.set("keyword", args[2]);

		Job sortJob = new Job(sortConf);
		Job searchJob = new Job(searchConf);

		searchJob.addDependingJob(sortJob);

		JobControl jobControl = new JobControl("Job Dependency");
		jobControl.addJob(searchJob);
		jobControl.addJob(sortJob);
		jobControl.run();

	}
}
