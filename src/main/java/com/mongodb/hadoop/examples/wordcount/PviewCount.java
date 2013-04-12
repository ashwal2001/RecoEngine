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

import java.io.*;
import java.util.*;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.bson.*;

import com.mongodb.hadoop.*;
import com.mongodb.hadoop.util.*;

public class PviewCount {

	private static final Log log = LogFactory.getLog(PviewCount.class);

	public static class TokenizerMapper extends
			Mapper<Object, BSONObject, Text, Text> {

		private final Text skuKey = new Text();
		private final Text sessionIdKey = new Text();

		public void map(Object key, BSONObject value, Context context)
				throws IOException, InterruptedException {

			System.out.println("key: " + key);
			System.out.println("value: " + value);

			if (value.get("time") != null && value.get("sessionId") != null
					&& value.get("sku") != null) {
				sessionIdKey.set(value.get("sessionId").toString());
				skuKey.set(value.get("sku").toString());
				context.write(sessionIdKey, skuKey);
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, Text, Text, Text> {

		private Text skuList = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			String str = "";
			for (final Text val : values) {
				str = val + ", " + str;
			}
			
			skuList.set(str);
			context.write(key, skuList);
		}
	}

	public static void main(String[] args) throws Exception {

		final Configuration conf = new Configuration();
		MongoConfigUtil.setInputURI(conf, "mongodb://localhost/products.pview");
		MongoConfigUtil.setOutputURI(conf, "mongodb://localhost/products.out2");
		System.out.println("Conf: " + conf);
		MongoConfigUtil.setCreateInputSplits(conf, false);
		args = new GenericOptionsParser(conf, args).getRemainingArgs();

		final Job job = new Job(conf, "word count");

		job.setJarByClass(PviewCount.class);

		job.setMapperClass(TokenizerMapper.class);

		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
