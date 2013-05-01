package com.xyz.reccommendation.jaccard;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class MRStage3New {
	/**
	 * Map to collect pairs
	 */
	public static class PairCollectMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
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
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String list = ival.toString();
			ArrayList<CPair> al = new ArrayList<CPair>();
			Scanner sc = new Scanner(list);
			// Converts the text string to CPairs
			while (sc.hasNext()) {
				String x = sc.next();
				if (sc.hasNext()) {
					int c = sc.nextInt();
					al.add(new CPair(x, c));
				}
			}
			// Iterate over the CPairs to create Text objects
			Text t = new Text();
			for (int i = 1; i < al.size(); i++) {
				String x = al.get(0).getX();
				String y = al.get(i).getX();
				if (x.compareTo(y) > 0)
					t.set(x + "|" + y);
				else
					t.set(y + "|" + x);
				String str = al.get(0).getCount() + "|" + al.get(i).getCount();
				Text val = new Text();
				val.set(str);
				// System.out.println(t + "###" + str);
				output.collect(t, val);
			}
		}
	}

	/**
	 * Reduce class that does the final normalization. Takes in data of the form
	 * (xi xj (sum of counts)) Outputs the similarity between xi and xj as (the
	 * number of y values xi and xj are associated with)/(the sum of the counts)
	 */
	public static class NormalizationReduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
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
		public void reduce(Text ikey, Iterator<Text> vlist,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			float count = 0;
			float count1 = 0, count2 = 0;
			float sumcounts = 0;

			while (vlist.hasNext()) {
				Text val = vlist.next();
				// System.out.println(val.toString());
				if (!val.toString().isEmpty()
						&& !val.toString().trim().isEmpty()) {
					String[] token = val.toString().split("\\|+");
					count1 = Float.parseFloat(token[0]);
					count2 = Float.parseFloat(token[1]);
					sumcounts = count1 + count2;
				}
				count++;
			}
			String[] keyArray = ikey.toString().split("\\|");

			float sumCosine = (float) (count / Math.sqrt(count1 * count2));
			float sumJaccard = (float) (count / (sumcounts - count));

			StringBuilder outputObj = new StringBuilder();
			outputObj.append("," + sumJaccard + ",");
			outputObj.append(sumCosine + ",");
			outputObj.append(keyArray[0] + ",");
			outputObj.append(keyArray[1]);
			Text t = new Text();
			t.set(outputObj.toString());
			output.collect(ikey, t);
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
		JobConf conf3 = new JobConf(MRStage3New.class);
		conf3.setJobName("Map-Reduce 3 with HDFS intermediate 3");
		conf3.setOutputKeyClass(Text.class);
		conf3.setOutputValueClass(Text.class);
		conf3.setMapperClass(PairCollectMap.class);
		conf3.setReducerClass(NormalizationReduce.class);
		conf3.setInputFormat(TextInputFormat.class);
		conf3.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf3, new Path("intermediate1"));
		FileOutputFormat.setOutputPath(conf3, new Path("intermediate2"));
		// MongoConfigUtil.setOutputURI(conf3,
		// "mongodb://54.251.196.236/products.out_stat");
		JobClient.runJob(conf3);
	}

}
