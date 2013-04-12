package com.mongodb.hadoop.examples.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobDependency {

	static public class WordMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		final private static IntWritable ONE = new IntWritable(1);
		private Text tokenValue = new Text();

		@Override
		protected void map(LongWritable offset, Text text, Context context)
				throws IOException, InterruptedException {
			for (String token : text.toString().split("\\s+")) {
				tokenValue.set(token);
				context.write(tokenValue, ONE);
			}
		}
	}

	static public class WordReducer extends
			Reducer<Text, LongWritable, Text, IntWritable> {
		private IntWritable total = new IntWritable();

		@Override
		protected void reduce(Text token, Iterable<LongWritable> counts,
				Context context) throws IOException, InterruptedException {
			int n = 0;
			for (LongWritable count : counts)
				n += count.get();
			total.set(n);
			context.write(token, total);
		}
	}

	public static class SortMapper extends
			Mapper<LongWritable, Text, DoubleWritable, DoubleWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			double dvalue;

			for (String token : tokens) {
				dvalue = Double.parseDouble(token);
				System.out.println("dvalue: " + dvalue);
				context.write(new DoubleWritable(dvalue), new DoubleWritable(
						dvalue));
			}
		}

	}

	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			Job job1 = new Job(conf, "job1 100");
			job1.setJarByClass(JobDependency.class);
			job1.setMapperClass(WordMapper.class);
			job1.setReducerClass(WordReducer.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job1, new Path(args[0]));
			String out = args[1] + System.nanoTime();
			FileOutputFormat.setOutputPath(job1, new Path(out));

			Configuration conf2 = new Configuration();
			Job job2 = new Job(conf2, "job2 200");
			job2.setJarByClass(JobDependency.class);
			job2.setOutputKeyClass(IntWritable.class);
			job2.setOutputValueClass(Text.class);
			job2.setMapperClass(SortMapper.class);
			job2.setReducerClass(Reducer.class);
			FileInputFormat.addInputPath(job2, new Path(out + "/part-r-00000"));
			FileOutputFormat.setOutputPath(job2, new Path(args[1]));

			ControlledJob controlledJob1 = new ControlledJob(
					job1.getConfiguration());
			ControlledJob controlledJob2 = new ControlledJob(
					job2.getConfiguration());
			controlledJob2.addDependingJob(controlledJob1);
			JobControl jobControl = new JobControl("control");
			jobControl.addJob(controlledJob1);
			jobControl.addJob(controlledJob2);
			Thread thread = new Thread(jobControl);
			thread.start();
			while (!jobControl.allFinished()) {
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			jobControl.stop();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
