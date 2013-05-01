package com.xyz.reccommendation.purchase;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import com.xyz.reccommendation.driver.SKU2SKUCount;

public class PurchaseRecoEngine {

	private static final Log log = LogFactory.getLog(SKU2SKUCount.class);

	public static class TokenizerMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable ONE = new IntWritable(1);
		private final Text word = new Text();

		public void map(LongWritable offset, Text text, Context context)
				throws IOException, InterruptedException {
			String[] array = text.toString().split("\\,");
			for (int i = 1; i < array.length - 1; i++) {
				if (isValid(array[i]) && isValid(array[i + 1])) {
					// System.out.println(array[i] + "####" + array[i + 1]);
					String sku1 = getSKU(array[i]);
					String sku2 = getSKU(array[i + 1]);
					// System.out.println(sku1 + "@@@@" + sku1);
					if (!sku1.equals(sku2)) {
						word.set(sku1 + "|" + sku2);
						// System.out.println(word);
						context.write(word, ONE);
					} else {
						// System.out.println("###########Same###############");
					}
				} else {
					// System.out.println("###########Invalid ###############"
					// + array[i] + "####" + array[i + 1] + "####");
				}
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, BSONWritable> {

		// private final IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			// System.out.println("key: " + key);
			// System.out.println("value: " + values);

			int sum = 0;
			for (final IntWritable val : values) {
				sum += val.get();
			}

			String[] keyArray = key.toString().split("\\|");

			// System.out.println("Count : " + sum + " p1 : " + keyArray[0]
			// + " p2 : " + keyArray[1]);

			BasicBSONObject output = new BasicBSONObject();
			output.put("count", sum);
			output.put("p1", keyArray[0]);
			output.put("p2", keyArray[1]);

			// result.set(sum);
			context.write(key, new BSONWritable(output));
		}
	}

	public static void main(String[] args) throws Exception {

		final Configuration conf = new Configuration();
		System.out.println("Conf: " + conf);

		MongoConfigUtil.setOutputURI(conf,
				"mongodb://54.251.196.236/products.out_stat_purchase");
		args = new GenericOptionsParser(conf, args).getRemainingArgs();

		final Job job = new Job(conf, "SKU To SKU count in purchase data");

		job.setJarByClass(PurchaseRecoEngine.class);

		job.setMapperClass(TokenizerMapper.class);

		job.setReducerClass(IntSumReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BSONWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path("inputPurchase"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	public static boolean isValid(final String string) {
		return string != null && !string.isEmpty() && !string.trim().isEmpty();
	}

	public static String getSKU(final String skuId) {
		String[] sku = skuId.split("\\-");
		return sku[0].replaceAll("\"", "");
	}
}
