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
import com.jabong.reccommendation.mapper.JaccardMapper;
import com.jabong.reccommendation.mapper.RecoEngineMapper;
import com.jabong.reccommendation.partitioner.CustomPartitioner;
import com.jabong.reccommendation.reducer.JaccardReducer;
import com.jabong.reccommendation.reducer.RecoEngineReducer;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class JaccardEngine {

	private static final Log log = LogFactory.getLog(JaccardEngine.class);

	public static void main(String[] args) throws Exception {

		final Configuration conf = new Configuration();
		MongoConfigUtil.setInputURI(conf, "mongodb://localhost/products.pview");
		MongoConfigUtil.setOutputURI(conf, "mongodb://localhost/products.out_jaccard");
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