package com.xyz.reccommendation.jaccard;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.xyz.reccommendation.jaccard.key.CompositeKeyComparator;
import com.xyz.reccommendation.jaccard.key.CompositeKeyGroupingComparator;
import com.xyz.reccommendation.jaccard.key.CompositeKeyPartitioner;
import com.xyz.reccommendation.jaccard.key.TextDoublePair;

public class CosineMRStage4New {
	/**
	 * Map to collect pairs
	 */
	public static class CosineMap extends MapReduceBase implements
			Mapper<LongWritable, Text, TextDoublePair, Text> {
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
		public void map(LongWritable ikey, Text ival,
				OutputCollector<TextDoublePair, Text> output, Reporter reporter)
				throws IOException {
			String[] token = ival.toString().split("\\,+");
			TextDoublePair okey = new TextDoublePair();
			okey.set(token[3], Double.parseDouble(token[2]));
			output.collect(okey, ival);
		}
	}

	/**
	 * Reduce class that does the final normalization. Takes in data of the form
	 * (xi xj (sum of counts)) Outputs the similarity between xi and xj as (the
	 * number of y values xi and xj are associated with)/(the sum of the counts)
	 */
	public static class CosineReduce extends MapReduceBase implements
			Reducer<TextDoublePair, Text, Text, BSONWritable> {
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
		public void reduce(TextDoublePair ikey, Iterator<Text> vlist,
				OutputCollector<Text, BSONWritable> output, Reporter reporter)
				throws IOException {
			int count = 0;
			while (vlist.hasNext() && count < 20) {
				Text val = vlist.next();
				String[] token = val.toString().split("\\,+");

				BasicBSONObject outputObj = new BasicBSONObject();
				outputObj.put("countCosine", token[2]);
				outputObj.put("p1", token[3]);
				outputObj.put("p2", token[4]);
				Text id = new Text(token[0].trim());

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
		JobConf conf3 = new JobConf(CosineMRStage4New.class);
		conf3.setJobName("Map-Reduce for Cosine 4");
		conf3.setMapOutputKeyClass(TextDoublePair.class);
		conf3.setMapOutputValueClass(Text.class);
		conf3.setOutputKeyClass(Text.class);
		conf3.setOutputValueClass(BSONWritable.class);

		conf3.setPartitionerClass(CompositeKeyPartitioner.class);
		conf3.setOutputKeyComparatorClass(CompositeKeyComparator.class);
		conf3.setOutputValueGroupingComparator(CompositeKeyGroupingComparator.class);

		conf3.setMapperClass(CosineMap.class);
		conf3.setReducerClass(CosineReduce.class);

		conf3.setInputFormat(TextInputFormat.class);
		conf3.setOutputFormat(MongoOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf3, new Path("intermediate2"));
		// MongoConfigUtil.setInputURI(conf3,
		// "mongodb://54.251.196.236/products.out_stat");
		// MongoConfigUtil.setCreateInputSplits(conf3, false);
		// FileOutputFormat.setOutputPath(conf3, new Path("output"));
		MongoConfigUtil.setOutputURI(conf3,
				"mongodb://54.251.196.236/products.out_stat_cosine");
		JobClient.runJob(conf3);
	}

}
