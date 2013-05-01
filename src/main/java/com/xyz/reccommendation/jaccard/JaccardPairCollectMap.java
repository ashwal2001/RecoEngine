package com.xyz.reccommendation.jaccard;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Map to collect pairs
 */
public class JaccardPairCollectMap extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	/**
	 * @param ikey
	 *            Dummy parameter required by Hadoop for the map operation, but
	 *            it does nothing.
	 * @param ival
	 *            Text used to read in necessary data, in this case each line of
	 *            the text will be a list of CPairs
	 * @param output
	 *            OutputCollector that collects the output of the map operation,
	 *            in this case a (Text, IntWritable) pair representing (
	 * @param reporter
	 *            Used by Hadoop, but we do not use it
	 */
	public void map(LongWritable ikey, Text ival, Context context)
			throws IOException, InterruptedException {
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
			context.write(t, new IntWritable(al.get(0).getCount()
					+ al.get(i).getCount()));
		}
	}
}