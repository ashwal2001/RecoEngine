package com.jabong.reccommendation.jaccard;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.bson.BSONObject;

import com.mongodb.hadoop.mapred.MongoInputFormat;
import com.mongodb.hadoop.mapred.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

/**
 * Map-Reduce job to find similarity between x’s i.e. similarity(xi,xj) =
 * (number of y’s they have in common)/(sum of y’s associated with xi and xj)
 * 
 * @author Ben Cole and Jacob Bank
 */
public class MapRed extends Configured implements Tool {
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

	/**
	 * Map class that takes in data of the form (x y count) and outputs data
	 * with y as the key and a CPair of (x,count) as the value
	 */
	public static class XYtoYXMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, CPair> {
		/**
		 * @param ikey
		 *            Dummy parameter required by Hadoop for the map operation,
		 *            but it does nothing.
		 * @param ival
		 *            Text used to read in necessary data, in this case each
		 *            line of the text will be in the format of ( x y count)
		 * @param output
		 *            OutputCollector that collects the output of the map
		 *            operation, in this case a (Text, CPair) pair representing
		 *            (y, (x,count))
		 * @param reporter
		 *            Used by Hadoop, but we do not use it
		 */
		public void map(LongWritable ikey, Text ival,
				OutputCollector<Text, CPair> output, Reporter reporter)
				throws IOException {
			StringTokenizer st = new StringTokenizer(ival.toString());
			String x = st.nextToken();
			String y = st.nextToken();
			String count = st.nextToken();
			Text okey = new Text(y);
			CPair oval = new CPair(x, Integer.parseInt(count));
			output.collect(okey, oval);
		}
	}

	/**
	 * Reduce class that splits a list of size n into many smaller lists of
	 * sizes 1 to chunk size + 1 This is done to further parallelise the pair
	 * collection process, which was a bottleneck in earlier versions due to the
	 * computational difficulty of performing an n-squared operation on a very
	 * long list, and also to balance the load by ensuring that no list is
	 * larger than the chunk size. Now, it will be many separate order of n
	 * operations.
	 */
	public static class SplitReduce extends MapReduceBase implements
			Reducer<Text, CPair, Text, Text> {
		/**
		 * @param ikey
		 *            Dummy parameter required by Hadoop.
		 * @param vlist
		 *            The list of (x,count) pairs associated with each y
		 * @param output
		 *            OutputCollector that collects a String representing a list
		 *            of (x,count) pairs
		 * @param reporter
		 *            Used by Hadoop, but we do not use it
		 */
		public void reduce(Text ikey, Iterator<CPair> vlist,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			Vector<String> chunks = new Vector<String>();
			final int c = 500;
			int counter = 0;
			Text key = new Text();
			Text value = new Text();
//			System.out.println(ikey.toString());
			while (vlist.hasNext()) {
				CPair cPair = vlist.next();
				String current = cPair.toString();
				for (String chunk : chunks) {
					// The key is modified to avoid collisions in the next
					// reduce.
					key.set("");
					value.set(current + " " + chunk);
//					System.out.println("@@@@"+current + " " + chunk);
					output.collect(key, value);
				}
				if (counter % c == 0)
					chunks.add(current);
				else
					chunks.set(chunks.size() - 1,
							current + " " + chunks.get(chunks.size() - 1));
//				System.out.println("####"+counter);
				counter++;
			}
		}
	}

	/**
	 * Map to collect pairs
	 */
	public static class PairCollectMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {
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
				OutputCollector<Text, IntWritable> output, Reporter reporter)
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
				output.collect(t, new IntWritable(al.get(0).getCount()
						+ al.get(i).getCount()));
			}
		}
	}

	/**
	 * Reduce class that does the final normalization. Takes in data of the form
	 * (xi xj (sum of counts)) Outputs the similarity between xi and xj as (the
	 * number of y values xi and xj are associated with)/(the sum of the counts)
	 */
	public static class NormalizationReduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, FloatWritable> {
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
		public void reduce(Text ikey, Iterator<IntWritable> vlist,
				OutputCollector<Text, FloatWritable> output, Reporter reporter)
				throws IOException {
			float count = 0;
			float sumcounts = 0;
			while (vlist.hasNext()) {
				sumcounts = (float) vlist.next().get();
				count++;
			}
			output.collect(ikey, new FloatWritable(count / (sumcounts - count)));
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
		int res = ToolRunner.run(new Configuration(), new MapRed(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		pass1();
		pass2();
		pass3();
		return 0;
	}

	public void pass1() throws Exception {
		// Job configuration
		JobConf conf = new JobConf(MapRed.class);
		conf.setJobName("Map-Reduce example");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(XYIdentityMap.class);
		conf.setReducerClass(CountYReduce.class);
//		conf.setInputFormat(TextInputFormat.class);
		conf.setInputFormat(MongoInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		// HDFS input and output directory
//		FileInputFormat.setInputPaths(conf, new Path("input"));
		MongoConfigUtil.setInputURI(conf, "mongodb://localhost/products.pview_simple");
		FileOutputFormat.setOutputPath(conf, new Path("intermediate0"));
		// Run map-reduce job
		JobClient.runJob(conf);
	}

	public void pass2() throws Exception {
		// Job configuration 2
		JobConf conf2 = new JobConf(MapRed.class);
		conf2.setJobName("Map-Reduce example 2");
		conf2.setOutputKeyClass(Text.class);
		conf2.setOutputValueClass(CPair.class);
		conf2.setMapperClass(XYtoYXMap.class);
		conf2.setReducerClass(SplitReduce.class);
		conf2.setInputFormat(TextInputFormat.class);
		conf2.setOutputFormat(TextOutputFormat.class);
		// HDFS input and output directory
		FileInputFormat.setInputPaths(conf2, new Path("intermediate0"));
		FileOutputFormat.setOutputPath(conf2, new Path("intermediate1"));
		// Run map-reduce job
		JobClient.runJob(conf2);
	}

	public void pass3() throws Exception {
		JobConf conf3 = new JobConf(MapRed.class);
		conf3.setJobName("Map-Reduce 3");
		conf3.setOutputKeyClass(Text.class);
		conf3.setOutputValueClass(IntWritable.class);
		conf3.setMapperClass(PairCollectMap.class);
		conf3.setReducerClass(NormalizationReduce.class);
		conf3.setInputFormat(TextInputFormat.class);
		conf3.setOutputFormat(MongoOutputFormat.class);
		FileInputFormat.setInputPaths(conf3, new Path("intermediate1"));
//		FileOutputFormat.setOutputPath(conf3, new Path("output"));
		MongoConfigUtil.setOutputURI(conf3, "mongodb://localhost/products.out_jaccard");
		JobClient.runJob(conf3);
	}

}
