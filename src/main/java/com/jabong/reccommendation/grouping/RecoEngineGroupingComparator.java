package com.jabong.reccommendation.grouping;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.jabong.reccommendation.key.CompositeKey;

public class RecoEngineGroupingComparator extends WritableComparator {

	protected RecoEngineGroupingComparator() {
		super(CompositeKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		CompositeKey key1 = (CompositeKey) w1;
		CompositeKey key2 = (CompositeKey) w2;

		// (check on SessionId)
		return key1.getSessionId().compareTo(key2.getSessionId());
	}

}
