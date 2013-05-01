package com.xyz.reccommendation.reducer;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JaccardReducer extends Reducer<Text, Text, Text, Text> {

	private static final Log log = LogFactory.getLog(JaccardReducer.class);

	private Text sessionIdList = new Text();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		String str = "";
		for (final Text val : values) {
			if (isValid(val.toString())) {
				if (str.trim().isEmpty()) {
					str = val.toString();
				} else {
					str = str + ", " + val.toString();
				}
			}
		}
		log.debug("######### " + str);
		log.debug("######### " + key);
		sessionIdList.set(str);
		context.write(key, sessionIdList);
	}

	public static boolean isValid(final String string) {
		return string != null && !string.isEmpty() && !string.trim().isEmpty();
	}

}
