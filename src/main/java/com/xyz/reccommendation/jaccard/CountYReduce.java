package com.xyz.reccommendation.jaccard;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reduce class that counts all of the y values for each x value
 */
public class CountYReduce extends Reducer<Text, Text, Text, CPair> {
	/**
	 * @param ikey
	 *            The x we are performing the y count on
	 * @param vlist
	 *            The list of y values associated with x
	 * @param output
	 *            OutputCollector that collects a (Text, CPair) pair, where the
	 *            Text is the x and the CPair is a (y,count) pair with count
	 *            being the total number of y values associated with x
	 * @param reporter
	 *            Used by Hadoop, but we do not use it
	 */
	@Override
	public void reduce(Text ikey, Iterable<Text> vlist, Context context)
			throws IOException, InterruptedException {
		// Traverse the <key, vlist>
		Set<Text> s = new TreeSet<Text>();
		for (final Text val : vlist) {
			// returns the same object with a different value each time
			s.add(val);
		}
		for (Text t : s) {
			context.write(ikey, new CPair(t.toString(), s.size()));
		}
	}
}
