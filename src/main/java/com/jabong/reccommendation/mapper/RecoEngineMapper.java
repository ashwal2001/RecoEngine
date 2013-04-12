package com.jabong.reccommendation.mapper;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import com.jabong.reccommendation.key.CompositeKey;

public class RecoEngineMapper extends
		Mapper<Object, BSONObject, CompositeKey, Text> {

	private static final Log log = LogFactory.getLog(RecoEngineMapper.class);

	private CompositeKey compositeKey = new CompositeKey();
	private Text valueSku = new Text();

	@Override
	public void map(Object key, BSONObject value, Context context)
			throws IOException, InterruptedException {

		log.debug("key: " + key);
		log.debug("value: " + value);
		if (value.get("time") != null && value.get("sessionId") != null
				&& value.get("sku") != null) {
			compositeKey.setDatetime(value.get("time").toString());
			compositeKey.setSessionId(value.get("sessionId").toString());

			valueSku.set(value.get("sku").toString());
			log.debug(value.get("sessionId").toString() + " : "
					+ value.get("sku"));
			context.write(compositeKey, valueSku);
		}

	}

}
