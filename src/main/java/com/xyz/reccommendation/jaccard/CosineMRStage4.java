package com.xyz.reccommendation.jaccard;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.MongoInputFormat;
import com.mongodb.hadoop.mapred.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.xyz.reccommendation.jaccard.key.CompositeKeyComparator;
import com.xyz.reccommendation.jaccard.key.CompositeKeyGroupingComparator;
import com.xyz.reccommendation.jaccard.key.CompositeKeyPartitioner;
import com.xyz.reccommendation.jaccard.key.TextDoublePair;

public class CosineMRStage4 {
	/**
	 * Map to collect pairs
	 */
	public static class CosineMap extends MapReduceBase implements
			Mapper<Object, BSONObject, TextDoublePair, BSONWritable> {
		/**
		 * @param ikey
		 *            Dummy parameter required by Hadoop for the map operation,
		 *            but it does nothing.
		 * @param ival
		 *            Text used to read in necessary data, in this case each
		 *            line of the text will be a list of CPairs
		 * @param output
		 *            OutputCollector that collects the output of the map
		 *            operation, in this case a (Text, IntWritable) pair
		 *            representing (
		 * @param reporter
		 *            Used by Hadoop, but we do not use it
		 */
		public void map(Object ikey, BSONObject ival,
				OutputCollector<TextDoublePair, BSONWritable> output,
				Reporter reporter) throws IOException {
			if (ival.get("countCosine") != null && ival.get("p1") != null) {
				TextDoublePair okey = new TextDoublePair();
				okey.set(ival.get("p1").toString(),
						Double.parseDouble(ival.get("countCosine").toString()));
				output.collect(okey, new BSONWritable(ival));
			}
		}
	}

	/**
	 * Reduce class that does the final normalization. Takes in data of the form
	 * (xi xj (sum of counts)) Outputs the similarity between xi and xj as (the
	 * number of y values xi and xj are associated with)/(the sum of the counts)
	 */
	public static class CosineReduce extends MapReduceBase implements
			Reducer<TextDoublePair, BSONObject, Text, BSONWritable> {
		/**
		 * @param ikey
		 *            The xi xj pair for which we are calculating the similarity
		 * @param vlist
		 *            The list of ((xi,xj),sum of counts) pairs
		 * @param output
		 *            OutputCollector that collects a (Text, FloatWritable)
		 *            pair. where the Text is the (xi,xj) pair and the
		 *            FloatWritable is their similarity
		 * @param reporter
		 *            Used by Hadoop, but we do not use it
		 */
		public void reduce(TextDoublePair ikey, Iterator<BSONObject> vlist,
				OutputCollector<Text, BSONWritable> output, Reporter reporter)
				throws IOException {
			int count = 0;
			while (vlist.hasNext() && count < 20) {
				BSONObject val = vlist.next();

				BasicBSONObject outputObj = new BasicBSONObject();
				outputObj.put("countCosine", val.get("countCosine").toString());
				outputObj.put("p1", val.get("p1").toString());
				outputObj.put("p2", val.get("p2").toString());

				Text id = new Text(val.get("_id").toString());

				output.collect(id, new BSONWritable(outputObj));// 7294, 50000,
																// jul 2013
				count++;
			}

		}

	}

	/**
	 * Main method for running the Hadoop MapReduce job
	 * 
	 * @param args
	 *            This main method takes no arguments
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		JobConf conf3 = new JobConf(CosineMRStage4.class);
		conf3.setJobName("Map-Reduce for Cosine 4");
		conf3.setMapOutputKeyClass(TextDoublePair.class);
		conf3.setMapOutputValueClass(BSONWritable.class);
		conf3.setOutputKeyClass(Text.class);
		conf3.setOutputValueClass(BSONWritable.class);

		conf3.setPartitionerClass(CompositeKeyPartitioner.class);
		conf3.setOutputKeyComparatorClass(CompositeKeyComparator.class);
		conf3.setOutputValueGroupingComparator(CompositeKeyGroupingComparator.class);

		conf3.setMapperClass(CosineMap.class);
		conf3.setReducerClass(CosineReduce.class);

		conf3.setInputFormat(MongoInputFormat.class);
		conf3.setOutputFormat(MongoOutputFormat.class);
		MongoConfigUtil.setInputURI(conf3,
				"mongodb://54.251.196.236/products.out_stat");
		// MongoConfigUtil.setCreateInputSplits(conf3, false);
		// FileOutputFormat.setOutputPath(conf3, new Path("output"));
		MongoConfigUtil.setOutputURI(conf3,
				"mongodb://54.251.196.236/products.out_stat_cosine");
		JobClient.runJob(conf3);
	}

}
