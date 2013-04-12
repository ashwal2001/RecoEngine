package com.jabong.reccommendation;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.bson.BSONObject;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.hadoop.examples.wordcount.WordCount;

import java.util.ArrayList;
import java.util.List;


public class WordCountTest {
//   MapReduceDriver<Object, BSONObject, Text, IntWritable, Text, IntWritable> mapReduceDriver;
//   MapDriver<Object, BSONObject, Text, IntWritable> mapDriver;
//   ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
//
//   @Before
//   public void setUp() {
//      WordCount.TokenizerMapper mapper = new WordCount.TokenizerMapper();
//      WordCount.IntSumReducer reducer = new WordCount.IntSumReducer();
//      mapDriver = new MapDriver<Object, BSONObject, Text, IntWritable>();
//      mapDriver.setMapper(mapper);
//      reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
//      reduceDriver.setReducer(reducer);
//      mapReduceDriver = new MapReduceDriver<Object, BSONObject, Text, IntWritable, Text, IntWritable>();
//      mapReduceDriver.setMapper(mapper);
//      mapReduceDriver.setReducer(reducer);
//   }
//
//  @Test
//   public void testMapper() {
////      mapDriver.withInput(new Object(""), new BSONObject("cat cat dog"));
////      mapDriver.withOutput(new Text("cat"), new LongWritable(1));
////      mapDriver.withOutput(new Text("cat"), new LongWritable(1));
////      mapDriver.withOutput(new Text("dog"), new LongWritable(1));
////      mapDriver.runTest();
//   }
//
//  /* @Test
//   public void testReducer() {
//      List<LongWritable> values = new ArrayList<LongWritable>();
//      values.add(new LongWritable(1));
//      values.add(new LongWritable(1));
//      reduceDriver.withInput(new Text("cat"), values);
//      reduceDriver.withOutput(new Text("cat"), new LongWritable(2));
//      reduceDriver.runTest();
//   }
//
//   @Test
//   public void testMapReduce() {
//      mapReduceDriver.withInput(new LongWritable(1), new Text("cat cat dog"));
//      mapReduceDriver.addOutput(new Text("cat"), new LongWritable(2));
//      mapReduceDriver.addOutput(new Text("dog"), new LongWritable(1));
//      mapReduceDriver.runTest();
//   }*/
}