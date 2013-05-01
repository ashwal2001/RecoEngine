package com.xyz.reccommendation.jaccard;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Map class that takes in data of the form (x y count) and outputs data with y
 * as the key and a CPair of (x,count) as the value
 */
public class XYtoYXMap extends Mapper<LongWritable, Text, Text, CPair> {
	/**
	 * @param ikey
	 *            Dummy parameter required by Hadoop for the map operation, but
	 *            it does nothing.
	 * @param ival
	 *            Text used to read in necessary data, in this case each line of
	 *            the text will be in the format of ( x y count)
	 * @param output
	 *            OutputCollector that collects the output of the map operation,
	 *            in this case a (Text, CPair) pair representing (y, (x,count))
	 * @param reporter
	 *            Used by Hadoop, but we do not use it
	 */
	public void map(LongWritable ikey, Text ival, Context context)
			throws IOException, InterruptedException {
		StringTokenizer st = new StringTokenizer(ival.toString());
		String x = st.nextToken();
		String y = st.nextToken();
		String count = st.nextToken();
		Text okey = new Text(y);
		CPair oval = new CPair(x, Integer.parseInt(count));
		context.write(okey, oval);
	}
}