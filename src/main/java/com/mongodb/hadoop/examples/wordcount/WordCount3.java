package com.mongodb.hadoop.examples.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount3 {

	static public class WordCountMapper extends
			Mapper<LongWritable, Text, Text, LongWritable> {
		final private static LongWritable ONE = new LongWritable(1);
		private Text tokenValue = new Text();

		@Override
		protected void map(LongWritable offset, Text text, Context context)
				throws IOException, InterruptedException {
			for (String token : text.toString().split("\\s+")) {
				tokenValue.set(token);
				context.write(tokenValue, ONE);
			}
		}
	}

	static public class WordCountReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable total = new LongWritable();

		@Override
		protected void reduce(Text token, Iterable<LongWritable> counts,
				Context context) throws IOException, InterruptedException {
			long n = 0;
			for (LongWritable count : counts)
				n += count.get();
			total.set(n);
			context.write(token, total);
		}
	}

	public static void main(String[] args) throws Exception {
		final Configuration conf = new Configuration();
		System.out.println( "Conf: " + conf );

        final Job job = new Job( conf, "word count" );
		job.setJarByClass(WordCount3.class);

		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
