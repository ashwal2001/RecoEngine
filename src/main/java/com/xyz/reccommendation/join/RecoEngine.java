/*
 * ===========================================================================
 * RecoEngine.java
 *
 * Created on 01-May-2013
 *
 * This code contains copyright information which is the proprietary property
 * of Jade eServices. No part of this code may be reproduced, stored or transmitted
 * in any form without the prior written permission of Jade eServices.
 *
 * Copyright (C) Jade eServices. 2013
 * All rights reserved.
 *
 * Modification history:
 * $Log: RecoEngine.java,v $
 * ===========================================================================
 */
package com.xyz.reccommendation.join;

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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.xyz.reccommendation.purchase.PurchaseRecoEngine;

/**
 * @author ashok
 * 
 * @version $Id: RecoEngine.java,v 1.1 01-May-2013 11:29:48 AM ashok Exp $
 */
public class RecoEngine {

	private static final Log log = LogFactory.getLog(RecoEngine.class);

	public static class PViewMapper extends
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
						System.out.println(sku1 + "|" + sku2);
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

	public static class SalesMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable TEN = new IntWritable(10);
		private final Text word = new Text();

		public void map(LongWritable offset, Text text, Context context)
				throws IOException, InterruptedException {
			String[] array = text.toString().split("\\,");
			for (int i = 1; i < array.length - 1; i++) {
				if (isValid(array[i]) && isValid(array[i + 1])) {
					String sku1 = getSKU(array[i]);
					String sku2 = getSKU(array[i + 1]);
					if (!sku1.equals(sku2)) {
						word.set(sku1 + "|" + sku2);
						context.write(word, TEN);
					} else {
						log.debug("## Same ##" + sku1 + " " + sku2);
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
		log.debug("Conf: " + conf);

		MongoConfigUtil.setOutputURI(conf,
				"mongodb://54.251.196.236/products.out_stat_join");
		args = new GenericOptionsParser(conf, args).getRemainingArgs();

		final Job job = new Job(conf,
				"SKU To SKU count in purchase and pview data");

		job.setJarByClass(PurchaseRecoEngine.class);

		job.setReducerClass(IntSumReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BSONWritable.class);

		job.setOutputFormatClass(MongoOutputFormat.class);

		MultipleInputs.addInputPath(job, new Path("inputPview"),
				TextInputFormat.class, PViewMapper.class);
		MultipleInputs.addInputPath(job, new Path("inputPurchase"),
				TextInputFormat.class, SalesMapper.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	public static boolean isValid(final String string) {
		return string != null && !string.isEmpty() && !string.trim().isEmpty();
	}

	public static String getSKU(final String skuId) {
		String sku = null;
		String[] articleSku = skuId.split("\\-");
		sku = articleSku[0].replaceAll("\"", "");
		return sku;
	}
}
