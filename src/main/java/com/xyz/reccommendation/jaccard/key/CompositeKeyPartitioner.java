package com.xyz.reccommendation.jaccard.key;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

import com.mongodb.hadoop.io.BSONWritable;

public class CompositeKeyPartitioner implements
		Partitioner<TextDoublePair, Text> {

	@Override
	public int getPartition(TextDoublePair key, Text val, int numPartitions) {
		int hash = key.getFirst().hashCode();
		int partition = Math.abs(hash) % numPartitions;
		return partition;
	}

	@Override
	public void configure(JobConf arg0) {
	}

}
