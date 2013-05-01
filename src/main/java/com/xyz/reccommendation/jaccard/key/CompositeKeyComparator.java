package com.xyz.reccommendation.jaccard.key;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {
	protected CompositeKeyComparator() {
		super(TextDoublePair.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		TextDoublePair k1 = (TextDoublePair) w1;
		TextDoublePair k2 = (TextDoublePair) w2;

		int result = k1.getFirst().compareTo(k2.getFirst());
		if (0 == result) {
			result = -1 * k1.getSecond().compareTo(k2.getSecond());
		}
		return result;
	}
}
