package com.xyz.reccommendation.reducer;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.xyz.reccommendation.key.CompositeKey;

public class RecoEngineReducer extends Reducer<CompositeKey, Text, Text, Text> {

	private static final Log log = LogFactory.getLog(RecoEngineReducer.class);

	private Text skuList = new Text();
	private Text sessionId = new Text();

	@Override
	public void reduce(CompositeKey key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		String str = "";
		for (final Text val : values) {
			if (isValid(val.toString()))
				str = str + "," + val.toString();
		}
		str = str.replaceFirst("," + "", "");
		log.debug("######### " + str);
		log.debug("######### " + key.getSessionId());
		String sessionIdStr = key.getSessionId();
		skuList.set(str);
		sessionId.set(sessionIdStr);
		context.write(sessionId, skuList);
	}

	public static boolean isValid(final String string) {
		return string != null && !string.isEmpty() && !string.trim().isEmpty();
	}

}
