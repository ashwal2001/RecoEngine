// SKU2SKUCount.java
/*
 * Copyright 2013 Jade eServices.
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

package com.xyz.reccommendation.driver;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;

/**
 * 
 */
public class SKU2SKUCount {

	private static final Log log = LogFactory.getLog(SKU2SKUCount.class);

	public static class TokenizerMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private final Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] array = value.toString().split("\\,");
			for (int i = 1; i < array.length - 1; i++) {
				if (isValid(array[i]) && isValid(array[i + 1])) {
					if (!array[i].equals(array[i + 1])) {
						String sku1 = array[i].replaceAll("\"", "");
						String sku2 = array[i + 1].replaceAll("\"", "");
						word.set(sku1 + "|" + sku2);
						log.debug(sku1 + "|" + sku2);
						context.write(word, one);
					} else {
						log.debug("## Same ##" + array[i] + " " + array[i + 1]);
					}
				} else {
					log.debug("## Invalid ##" + array[i] + " " + array[i + 1]);
				}
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, BSONWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			log.debug("Key : " + key.toString());
			int sum = 0;
			for (final IntWritable val : values) {
				sum += val.get();
			}

			String[] keyArray = key.toString().split("\\|");

			log.debug("Count : " + sum + " p1 : " + keyArray[0] + " p2 : "
					+ keyArray[1]);

			BasicBSONObject output = new BasicBSONObject();
			output.put("count", sum);
			output.put("p1", keyArray[0]);
			output.put("p2", keyArray[1]);

			context.write(key, new BSONWritable(output));
		}
	}

	public static void main(String[] args) throws Exception {

		final Configuration conf = new Configuration();
		MongoConfigUtil.setOutputURI(conf,
				"mongodb://54.251.196.236/products.out_stat_custom");
		log.debug("Conf: " + conf);
		MongoConfigUtil.setCreateInputSplits(conf, false);
		args = new GenericOptionsParser(conf, args).getRemainingArgs();

		final Job job = new Job(conf,
				"Count the sku to sku mapping from pview data on hdfs in \"inputPview\" path.");

		job.setJarByClass(SKU2SKUCount.class);

		job.setMapperClass(TokenizerMapper.class);

		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BSONWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("inputPview"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	public static boolean isValid(final String string) {
		return string != null && !string.isEmpty() && !string.trim().isEmpty();
	}
}
