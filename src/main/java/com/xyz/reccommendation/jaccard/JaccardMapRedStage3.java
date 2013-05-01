package com.xyz.reccommendation.jaccard;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class JaccardMapRedStage3 {

	private static final Log log = LogFactory.getLog(JaccardMapRedStage3.class);

	public static void main(String[] args) throws Exception {

		final Configuration conf = new Configuration();
		MongoConfigUtil.setInputURI(conf,
				"mongodb://54.251.196.236/products.out_Stage_2");
		MongoConfigUtil.setOutputURI(conf,
				"mongodb://54.251.196.236/products.out_jaccard");
		MongoConfigUtil.setCreateInputSplits(conf, false);
		args = new GenericOptionsParser(conf, args).getRemainingArgs();

		log.debug("Conf: " + conf);

		final Job job = new Job(conf, "Map Reduce Stage 3");

		job.setJarByClass(JaccardMapRedStage3.class);

		job.setMapperClass(JaccardPairCollectMap.class);

		job.setReducerClass(JaccardNormalizationReduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
