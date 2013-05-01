package com.mongodb.hadoop.examples.wordcount;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
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
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;

public class TotalSortDemo {
	enum MyCounters {
		MAPFUNCTIONCALLS, REDUCEFUNCTIONCALLS
	}

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			reporter.incrCounter(MyCounters.MAPFUNCTIONCALLS, 1);
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
			}

		}

	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			reporter.incrCounter(MyCounters.REDUCEFUNCTIONCALLS, 1);
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}

	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(TotalSortDemo.class);
		conf.setJobName("TotalSortDemo");
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setNumReduceTasks(4);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		//conf.setInputFormat(SequenceFileInputFormat.class);
//		conf.setOutputKeyClass(IntWritable.class);
//		conf.setOutputFormat(SequenceFileOutputFormat.class);
//		SequenceFileOutputFormat.setCompressOutput(conf, true);
//		SequenceFileOutputFormat
//				.setOutputCompressorClass(conf, GzipCodec.class);
//		SequenceFileOutputFormat.setOutputCompressionType(conf,
//				CompressionType.BLOCK);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		// LazyOutputFormat.setOutputFormatClass(new
		// Job(),TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

//		InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<IntWritable, Text>(
//				0.1, 10000, 10);

		Path partitionFile = new Path(new Path(args[0]), "_partitions");
		TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
//		InputSampler.writePartitionFile(conf, sampler);

		// Customizing Partition.........
		conf.setPartitionerClass(TotalOrderPartitioner.class);

		// Add to DistributedCache
		URI partitionUri = new URI(partitionFile.toString() + "#_partitions");
		DistributedCache.addCacheFile(partitionUri, conf);
		DistributedCache.createSymlink(conf);
		JobClient.runJob(conf);
	}
}