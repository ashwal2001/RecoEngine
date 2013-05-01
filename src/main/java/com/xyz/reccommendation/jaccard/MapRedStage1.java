package com.xyz.reccommendation.jaccard;

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

public class MapRedStage1 {

	private static final Log log = LogFactory.getLog(MapRedStage1.class);

	public static void main(String[] args) throws Exception {
		final Configuration conf = new Configuration();
		final Job job = new Job(conf, "Map Reduce Stage 1");
//		Properties prop = new Properties();
//
//		try {
//			String envt = System.getProperty("envtType");
//
//			// if (!isValid(envt)) {
//			envt = "dev";
//			// }
//			System.out.println("@@@@" + envt);
//			// load a properties file from class path, inside static method
//			prop.load(MapRedStage1.class.getClassLoader().getResourceAsStream(
//					"/home/ashok/data/dev/hadoop/RecoEngine/src/main/resources/config-" + envt + ".properties"));
//
//		} catch (IOException ex) {
//			ex.printStackTrace();
//		}

//		MongoConfigUtil.setCreateInputSplits(conf, false);
		args = new GenericOptionsParser(conf, args).getRemainingArgs();

		log.debug("Conf: " + conf);

		job.setJarByClass(MapRedStage1.class);

		job.setMapperClass(XYIdentityMap.class);

		job.setReducerClass(CountYReduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		MongoConfigUtil.setInputURI(conf,
				"mongodb://localhost/products.pview");
//		MongoConfigUtil.setInputURI(
//				conf,
//				"mongodb://" + prop.getProperty("mongodb.ip") + "/"
//						+ prop.getProperty("mongodb.dbname") + "."
//						+ prop.getProperty("mongodb.collectionname.input"));
		FileOutputFormat.setOutputPath(job, new Path("intermediate0"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static boolean isValid(final String string) {
		return string != null && !string.isEmpty() && !string.trim().isEmpty();
	}

}
