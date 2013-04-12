package com.jabong.reccommendation.driver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import com.jabong.reccommendation.grouping.CompositeKeyComparator;
import com.jabong.reccommendation.grouping.RecoEngineGroupingComparator;
import com.jabong.reccommendation.key.CompositeKey;
import com.jabong.reccommendation.mapper.RecoEngineMapper;
import com.jabong.reccommendation.partitioner.CustomPartitioner;
import com.jabong.reccommendation.reducer.RecoEngineReducer;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class RecoEngine {

	private static final Log log = LogFactory.getLog(RecoEngine.class);

	public static void main(String[] args) throws Exception {

		final Configuration conf = new Configuration();
		MongoConfigUtil.setInputURI(conf, "mongodb://localhost/products.pview_simple");
		MongoConfigUtil.setOutputURI(conf, "mongodb://localhost/products.out_unsupervised_1");
		MongoConfigUtil.setCreateInputSplits(conf, false);
		args = new GenericOptionsParser(conf, args).getRemainingArgs();
		// String color = conf.get("color");
		// System.out.println("color = " + color);
		log.debug("Conf: " + conf);

		final Job job = new Job(conf, "Reco Engine");

		job.setJarByClass(RecoEngine.class);

		job.setMapperClass(RecoEngineMapper.class);

		// job.setCombinerClass(RecoEngineReducer.class);
		job.setReducerClass(RecoEngineReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);

		job.setMapOutputKeyClass(CompositeKey.class);
		job.setPartitionerClass(CustomPartitioner.class);
		job.setGroupingComparatorClass(RecoEngineGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}