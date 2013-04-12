package com.mongodb.hadoop.examples.wordcount;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChainDriver {
	// public static Logger log = Logger.getLogger(ChainDriver.class);

	private static final Log log = LogFactory.getLog(DataCount.class);

	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		
		// Start main Chain Job and declare its conf and job
		Configuration chainConf = new Configuration();
		
		Job chainJob = Job.getInstance(chainConf);
		// Variable names kept like conf1 etc to make code less cluttered
		// Start Mapper for MyMapperA
		Configuration conf1 = new Configuration(false);
		
		// Example for Passing arguments to the mappers
		conf1.set("myParameter", args[2]);
		
//		ChainMapper.addMapper(chainJob, MyMapperA.class, LongWritable.class,
//				Text.class, Text.class, Text.class, conf1);
		// Start Mapper for Second replacement
		Configuration conf2 = new Configuration(false);
		// Dynamically take the class name from argument to make more Dynamic
		// chain :)
		// (MapperC OR MapperD)
//		ChainMapper.addMapper(chainJob,
//				(Class<? extends Mapper>) Class.forName(args[2]), Text.class,
//				Text.class, NullWritable.class, Text.class, conf2);
		// Set the parameters for main Chain Job
		chainJob.setJarByClass(ChainDriver.class);
		FileInputFormat.addInputPath(chainJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(chainJob, new Path(args[1]));
		System.exit(chainJob.waitForCompletion(true) ? 0 : 1);
	}
}
