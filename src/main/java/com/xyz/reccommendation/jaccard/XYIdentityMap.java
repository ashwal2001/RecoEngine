package com.xyz.reccommendation.jaccard;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

/**
 * Map class that maps a text input of x y to a text output of x y
 */
public class XYIdentityMap extends Mapper<Object, BSONObject, Text, Text> {
	/**
	 * @param ikey
	 *            Dummy parameter required by Hadoop for the map operation, but
	 *            it does nothing.
	 * @param ival
	 *            Text used to read in necessary data, in this case each line of
	 *            the text will be in the format of ( x y )
	 * @param output
	 *            OutputCollector that collects the output of the map operation,
	 *            in this case a (Text, Text) pair representing the (x,y)
	 * @param reporter
	 *            Used by Hadoop, but we do not use it
	 */
	@Override
	public void map(Object ikey, BSONObject ival, Context context)
			throws IOException, InterruptedException {
		if (ival.get("time") != null && ival.get("sessionId") != null
				&& ival.get("sku") != null) {
			Text okey = new Text(ival.get("sku").toString());
			Text oval = new Text(ival.get("sessionId").toString());
			context.write(okey, oval);
		}
	}
}