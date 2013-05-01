package com.xyz.reccommendation.jaccard;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.io.BSONWritable;

/**
 * Reduce class that does the final normalization. Takes in data of the form (xi
 * xj (sum of counts)) Outputs the similarity between xi and xj as (the number
 * of y values xi and xj are associated with)/(the sum of the counts)
 */
public class JaccardNormalizationReduce extends
		Reducer<Text, IntWritable, Text, BSONWritable> {
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
	public void reduce(Text ikey, Iterable<IntWritable> vlist, Context context)
			throws IOException, InterruptedException {
		float count = 0;
		float sumcounts = 0;
		for (final IntWritable val : vlist) {
			sumcounts = val.get();
			count++;
		}

		String[] keyArray = ikey.toString().split("\\|");

		float sum = (float) (count / (sumcounts - count));

		BasicBSONObject outputObj = new BasicBSONObject();
		outputObj.put("count", sum);
		outputObj.put("p1", keyArray[0]);
		outputObj.put("p2", keyArray[1]);

		context.write(ikey, new BSONWritable(outputObj));
	}
}