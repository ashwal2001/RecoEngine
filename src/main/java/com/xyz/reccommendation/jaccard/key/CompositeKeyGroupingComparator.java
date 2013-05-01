package com.xyz.reccommendation.jaccard.key;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyGroupingComparator extends WritableComparator {
	protected CompositeKeyGroupingComparator() {
		super(TextDoublePair.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		TextDoublePair k1 = (TextDoublePair) w1;
		TextDoublePair k2 = (TextDoublePair) w2;

		return k1.getFirst().compareTo(k2.getFirst());
	}
}
