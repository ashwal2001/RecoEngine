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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

/**
 * test.in db.in.insert( { x : "eliot was here" } ) db.in.insert( { x :
 * "eliot is here" } ) db.in.insert( { x : "who is here" } ) =
 */
public class CopyOfDataCount {

//	private static final Log log = LogFactory.getLog(DataCount.class);
//
//	public static class TokenizerMapper extends
//			Mapper<Object, BSONObject, Text, IntWritable> {
//
//		private final static IntWritable one = new IntWritable(1);
//		private final Text word = new Text();
//
//		public void map(Object key, BSONObject value, Context context)
//				throws IOException, InterruptedException {
//
//			System.out.println("key: " + key);
//			System.out.println("value: " + value);
//
//			String text = value.get("value").toString();
//
//			// NI091MA32QWTINDFAS, NI091MA28QWXINDFAS, NI091MA44QWHINDFAS,
//			// UN573MA24POLINDFAS,
//
//			String[] array = text.split("\\, ");
//			System.out.println(array.length);
//			for (int i = 1; i < array.length - 1; i++) {
//				System.out.println(array[i] + " : " + array[i + 1]);
//				if (isValid(array[i]) && isValid(array[i + 1])) {
//					if (!array[i].equals(array[i + 1])) {
//						word.set(array[i] + " : " + array[i + 1]);
//						context.write(word, one);
//					} else {
//						System.out.println("###########Same###############");
//					}
//				} else {
//					System.out.println("###########Invalid ###############"
//							+ array[i] + "####" + array[i + 1] + "####");
//				}
//			}
//		}
//	}
//
//	public static class IntSumReducer extends
//			Reducer<Text, IntWritable, Text, IntWritable> {
//
//		private final IntWritable result = new IntWritable();
//
//		public void reduce(Text key, Iterable<IntWritable> values,
//				Context context) throws IOException, InterruptedException {
//
//			System.out.println("key: " + key);
//			System.out.println("value: " + values);
//
//			int sum = 0;
//			for (final IntWritable val : values) {
//				sum += val.get();
//			}
//			result.set(sum);
//			context.write(key, result);
//		}
//	}
//
//	public static void main(String[] args) throws Exception {
//
//		final Configuration conf = new Configuration();
//		MongoConfigUtil.setInputURI(conf, "mongodb://localhost/products.out2");
//		MongoConfigUtil
//				.setOutputURI(conf, "mongodb://localhost/products.out21");
//		System.out.println("Conf: " + conf);
//		MongoConfigUtil.setCreateInputSplits(conf, false);
//		args = new GenericOptionsParser(conf, args).getRemainingArgs();
//
//		final Job job = new Job(conf, "data count");
//
//		job.setJarByClass(DataCount.class);
//
//		job.setMapperClass(TokenizerMapper.class);
//
//		job.setCombinerClass(IntSumReducer.class);
//		job.setReducerClass(IntSumReducer.class);
//
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
//
//		job.setInputFormatClass(MongoInputFormat.class);
//		job.setOutputFormatClass(MongoOutputFormat.class);
//
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
//
//	}
//
//	public static boolean isValid(final String string) {
//		return string != null && !string.isEmpty() && !string.trim().isEmpty();
//	}
}
