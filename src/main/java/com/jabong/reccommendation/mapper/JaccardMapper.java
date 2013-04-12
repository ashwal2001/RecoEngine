package com.jabong.reccommendation.mapper;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

public class JaccardMapper extends Mapper<Object, BSONObject, Text, Text> {

	private static final Log log = LogFactory.getLog(JaccardMapper.class);

	private Text sessionId = new Text();
	private Text valueSku = new Text();

	@Override
	public void map(Object key, BSONObject value, Context context)
			throws IOException, InterruptedException {

		log.debug("key: " + key);
		log.debug("value: " + value);
		if (value.get("sessionId") != null && value.get("sku") != null) {

			valueSku.set(value.get("sku").toString());
			sessionId.set(value.get("sessionId").toString());
			log.debug(value.get("sessionId").toString() + " : "
					+ value.get("sku"));
			context.write(valueSku, sessionId);
		}

	}

}
