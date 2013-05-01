// WordCount.java
/*
 * Copyright 2010 10gen Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.hadoop.examples.wordcount;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.GenericOptionsParser;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;

/**
 * test.in db.in.insert( { x : "eliot was here" } ) db.in.insert( { x :
 * "eliot is here" } ) db.in.insert( { x : "who is here" } ) =
 */
public class DataCountNew {

	private static final Log log = LogFactory.getLog(DataCountNew.class);

	public static class TokenizerMapper extends
			Mapper<Object, BSONObject, Text, BSONWritable> {

//		private final static IntWritable one = new IntWritable(1);
		private final Text word = new Text();

		public void map(Object key, BSONObject value, Context context)
				throws IOException, InterruptedException {

			log.debug("Map key: " + key);
			System.out.println("Map value: " + value);

			String text = value.get("value").toString();
			String keytext = value.get("_id").toString();

			String[] keyArray = keytext.toString().split("\\|");

			System.out.println("Count : " + text + " p1 : " + keyArray[0]
					+ " p2 : " + keyArray[1]);

			BasicBSONObject output = new BasicBSONObject();
			output.put("count", text);
			output.put("p1", keyArray[0]);
			output.put("p2", keyArray[1]);
//			word.set(keyArray[0]);
			
			word.set(keytext);
//			context.write(word, new IntWritable(new Integer(text)));
			context.write(word, new BSONWritable(output));
		}
	}

	public static void main(String[] args) throws Exception {

		final Configuration conf = new Configuration();
		MongoConfigUtil.setInputURI(conf, "mongodb://localhost/products.out21");
		MongoConfigUtil.setOutputURI(conf,
				"mongodb://localhost/products.out211");

		System.out.println("Conf: " + conf);
		MongoConfigUtil.setCreateInputSplits(conf, false);
		args = new GenericOptionsParser(conf, args).getRemainingArgs();

		final Job job = new Job(conf, "data count new");

		job.setJarByClass(DataCountNew.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BSONWritable.class);

		job.setMapperClass(TokenizerMapper.class);

		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	public static boolean isValid(final String string) {
		return string != null && !string.isEmpty() && !string.trim().isEmpty();
	}
}
