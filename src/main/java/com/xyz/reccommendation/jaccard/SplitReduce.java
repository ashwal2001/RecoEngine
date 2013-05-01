package com.xyz.reccommendation.jaccard;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reduce class that splits a list of size n into many smaller lists of sizes 1
 * to chunk size + 1 This is done to further parallelise the pair collection
 * process, which was a bottleneck in earlier versions due to the computational
 * difficulty of performing an n-squared operation on a very long list, and also
 * to balance the load by ensuring that no list is larger than the chunk size.
 * Now, it will be many separate order of n operations.
 */
public class SplitReduce extends Reducer<Text, CPair, Text, Text> {
	/**
	 * @param ikey
	 *            Dummy parameter required by Hadoop.
	 * @param vlist
	 *            The list of (x,count) pairs associated with each y
	 * @param output
	 *            OutputCollector that collects a String representing a list of
	 *            (x,count) pairs
	 * @param reporter
	 *            Used by Hadoop, but we do not use it
	 */
	@Override
	public void reduce(Text ikey, Iterable<CPair> vlist, Context context)
			throws IOException, InterruptedException {
		Vector<String> chunks = new Vector<String>();
		final int c = 500;
		int counter = 0;
		Text key = new Text();
		Text value = new Text();
		// System.out.println(ikey.toString());
		for (final CPair cPair : vlist) {
			String current = cPair.toString();
			for (String chunk : chunks) {
				// The key is modified to avoid collisions in the next
				// reduce.
				key.set("");
				value.set(current + " " + chunk);
				// System.out.println("@@@@"+current + " " + chunk);
				context.write(key, value);
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