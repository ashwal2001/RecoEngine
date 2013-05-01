package com.xyz.reccommendation.jaccard;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.bson.BSONObject;

import com.mongodb.hadoop.mapred.MongoInputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class MRStage1 {
	/**
	 * Map class that maps a text input of x y to a text output of x y
	 */
	public static class XYIdentityMap extends MapReduceBase implements
			Mapper<Object, BSONObject, Text, Text> {
		/**
		 * @param ikey
		 *            Dummy parameter required by Hadoop for the map operation,
		 *            but it does nothing.
		 * @param ival
		 *            Text used to read in necessary data, in this case each
		 *            line of the text will be in the format of ( x y )
		 * @param output
		 *            OutputCollector that collects the output of the map
		 *            operation, in this case a (Text, Text) pair representing
		 *            the (x,y)
		 * @param reporter
		 *            Used by Hadoop, but we do not use it
		 */

		public void map(Object ikey, BSONObject ival,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			if (ival.get("time") != null && ival.get("sessionId") != null
					&& ival.get("sku") != null) {
				Text okey = new Text(ival.get("sku").toString());
				Text oval = new Text(ival.get("sessionId").toString());
				output.collect(okey, oval);
			}
		}
	}

	/**
	 * Reduce class that counts all of the y values for each x value
	 */
	public static class CountYReduce extends MapReduceBase implements
			Reducer<Text, Text, Text, CPair> {
		/**
		 * @param ikey
		 *            The x we are performing the y count on
		 * @param vlist
		 *            The list of y values associated with x
		 * @param output
		 *            OutputCollector that collects a (Text, CPair) pair, where
		 *            the Text is the x and the CPair is a (y,count) pair with
		 *            count being the total number of y values associated with x
		 * @param reporter
		 *            Used by Hadoop, but we do not use it
		 */
		public void reduce(Text ikey, Iterator<Text> vlist,
				OutputCollector<Text, CPair> output, Reporter reporter)
				throws IOException {
			// Traverse the <key, vlist>
			Set<Text> s = new TreeSet<Text>();
			while (vlist.hasNext()) {
				Text t = vlist.next(); // returns the same object with a
										// different value each time
				s.add(new Text(t));
			}
			for (Text t : s) {
				output.collect(ikey, new CPair(t.toString(), s.size()));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		// Job configuration
		JobConf conf = new JobConf(MRStage1.class);
		conf.setJobName("Map-Reduce Stage 1");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(XYIdentityMap.class);
		conf.setReducerClass(CountYReduce.class);
		// conf.setInputFormat(TextInputFormat.class);
		conf.setInputFormat(MongoInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		// HDFS input and output directory
		// FileInputFormat.setInputPaths(conf, new Path("input"));
		MongoConfigUtil
				.setInputURI(conf, "mongodb://54.251.196.236/products.pview");
		MongoConfigUtil.setCreateInputSplits(conf, false);
		FileOutputFormat.setOutputPath(conf, new Path("intermediate0"));
		// Run map-reduce job
		JobClient.runJob(conf);
	}
}
