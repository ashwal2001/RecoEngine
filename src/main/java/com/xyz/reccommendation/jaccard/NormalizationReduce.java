package com.xyz.reccommendation.jaccard;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.io.BSONWritable;

/**
 * Reduce class that does the final normalization. Takes in data of the form (xi
 * xj (sum of counts)) Outputs the similarity between xi and xj as (the number
 * of y values xi and xj are associated with)/(the sum of the counts)
 */
public class NormalizationReduce extends
		Reducer<Text, Text, Text, BSONWritable> {
	/**
	 * @param ikey
	 *            The xi xj pair for which we are calculating the similarity
	 * @param vlist
	 *            The list of ((xi,xj),sum of counts) pairs
	 * @param output
	 *            OutputCollector that collects a (Text, FloatWritable) pair.
	 *            where the Text is the (xi,xj) pair and the FloatWritable is
	 *            their similarity
	 * @param reporter
	 *            Used by Hadoop, but we do not use it
	 */
	@Override
	public void reduce(Text ikey, Iterable<Text> vlist, Context context)
			throws IOException, InterruptedException {
		float count = 0;
		float count1 = 0, count2 = 0;

		for (final Text val : vlist) {
			// System.out.println(val.toString());
			if (!val.toString().isEmpty() && !val.toString().trim().isEmpty()) {
				String[] token = val.toString().split("\\|+");
				count1 = Float.parseFloat(token[0]);
				count2 = Float.parseFloat(token[1]);
			}
			count++;
		}
		String[] keyArray = ikey.toString().split("\\|");

		float sum = (float) (count / Math.sqrt(count1 * count2));

		BasicBSONObject outputObj = new BasicBSONObject();
		outputObj.put("count", sum);
		outputObj.put("p1", keyArray[0]);
		outputObj.put("p2", keyArray[1]);

		context.write(ikey, new BSONWritable(outputObj));
	}
}