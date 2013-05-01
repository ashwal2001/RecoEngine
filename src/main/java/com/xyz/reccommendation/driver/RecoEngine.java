package com.xyz.reccommendation.driver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.xyz.reccommendation.grouping.CompositeKeyComparator;
import com.xyz.reccommendation.grouping.RecoEngineGroupingComparator;
import com.xyz.reccommendation.key.CompositeKey;
import com.xyz.reccommendation.mapper.RecoEngineMapper;
import com.xyz.reccommendation.partitioner.CustomPartitioner;
import com.xyz.reccommendation.reducer.RecoEngineReducer;

public class RecoEngine {

	private static final Log log = LogFactory.getLog(RecoEngine.class);

	public static void main(String[] args) throws Exception {

		final Configuration conf = new Configuration();
		MongoConfigUtil.setInputURI(conf,
				"mongodb://54.251.196.236/products.pview");
		MongoConfigUtil.setCreateInputSplits(conf, false);
		args = new GenericOptionsParser(conf, args).getRemainingArgs();
		// String color = conf.get("color");
		// System.out.println("color = " + color);
		log.debug("Conf: " + conf);

		final Job job = new Job(conf, "MR Job for grouping records as per session id and time");

		job.setJarByClass(RecoEngine.class);

		job.setMapperClass(RecoEngineMapper.class);

		// job.setCombinerClass(RecoEngineReducer.class);
		job.setReducerClass(RecoEngineReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(CompositeKey.class);
		job.setPartitionerClass(CustomPartitioner.class);
		job.setGroupingComparatorClass(RecoEngineGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);

		FileOutputFormat.setOutputPath(job, new Path("inputPview"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}