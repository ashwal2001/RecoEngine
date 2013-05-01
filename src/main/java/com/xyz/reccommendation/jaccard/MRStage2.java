package com.xyz.reccommendation.jaccard;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Vector;

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

public class MRStage2 {

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
			// System.out.println(ikey.toString());
			while (vlist.hasNext()) {
				CPair cPair = vlist.next();
				String current = cPair.toString();
				for (String chunk : chunks) {
					// The key is modified to avoid collisions in the next
					// reduce.
					key.set("");
					value.set(current + " " + chunk);
					// System.out.println("@@@@"+current + " " + chunk);
					output.collect(key, value);
				}
				if (counter % c == 0)
					chunks.add(current);
				else
					chunks.set(chunks.size() - 1,
							current + " " + chunks.get(chunks.size() - 1));
				// System.out.println("####"+counter);
				counter++;
			}
		}
	}

	public static void main(String[] args) throws Exception {
		// Job configuration 2
		JobConf conf2 = new JobConf(MRStage2.class);
		conf2.setJobName("Map-Reduce Stage 2");
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

}
