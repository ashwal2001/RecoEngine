package com.xyz.reccommendation.jaccard;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class CosineMapRedStage3 {

	private static final Log log = LogFactory.getLog(CosineMapRedStage3.class);

	public static void main(String[] args) throws Exception {

		Properties prop = new Properties();

		try {
			String envt = System.getProperty("envtType");
			if (!isValid(envt)){
				envt = "dev";
			}
			// load a properties file from class path, inside static method
			prop.load(CosineMapRedStage3.class.getClassLoader().getResourceAsStream(
					"config-" + envt + ".properties"));

		} catch (IOException ex) {
			ex.printStackTrace();
		}

		final Configuration conf = new Configuration();
		MongoConfigUtil.setInputURI(conf,
				"mongodb://"+prop.getProperty("mongodb.ip")+"/"+prop.getProperty("mongodb.dbname")+".out_Stage_2");
		MongoConfigUtil.setOutputURI(conf,
				"mongodb://"+prop.getProperty("mongodb.ip")+"/"+prop.getProperty("mongodb.dbname")+".out_cosine");
		MongoConfigUtil.setCreateInputSplits(conf, false);
		args = new GenericOptionsParser(conf, args).getRemainingArgs();

		log.debug("Conf: " + conf);

		final Job job = new Job(conf, "Map Reduce Stage 2");

		job.setJarByClass(CosineMapRedStage3.class);

		job.setMapperClass(PairCollectMap.class);

		job.setReducerClass(NormalizationReduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(CPair.class);

		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static boolean isValid(final String string) {
		return string != null && !string.isEmpty() && !string.trim().isEmpty();
	}


}
