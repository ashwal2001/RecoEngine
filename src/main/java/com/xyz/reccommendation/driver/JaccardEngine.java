package com.xyz.reccommendation.driver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.xyz.reccommendation.mapper.JaccardMapper;
import com.xyz.reccommendation.reducer.JaccardReducer;

public class JaccardEngine {

	private static final Log log = LogFactory.getLog(JaccardEngine.class);

	public static void main(String[] args) throws Exception {

		final Configuration conf = new Configuration();
		MongoConfigUtil.setInputURI(conf, "mongodb://localhost/products.pview");
		MongoConfigUtil.setOutputURI(conf,
				"mongodb://localhost/products.out_jaccard");
		MongoConfigUtil.setCreateInputSplits(conf, false);
		args = new GenericOptionsParser(conf, args).getRemainingArgs();

		log.debug("Conf: " + conf);

		final Job job = new Job(conf, "Reco Engine");

		job.setJarByClass(JaccardEngine.class);

		job.setMapperClass(JaccardMapper.class);

		// job.setCombinerClass(RecoEngineReducer.class);
		job.setReducerClass(JaccardReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}