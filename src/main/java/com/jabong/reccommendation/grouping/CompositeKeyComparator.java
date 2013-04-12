package com.jabong.reccommendation.grouping;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.jabong.reccommendation.key.CompositeKey;

public class CompositeKeyComparator extends WritableComparator {
	protected CompositeKeyComparator() {
		super(CompositeKey.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		CompositeKey key1 = (CompositeKey) w1;
		CompositeKey key2 = (CompositeKey) w2;

		// (first check on SessionId)
		int compare = key1.getSessionId().compareTo(key2.getSessionId());

		if (compare == 0) {
			// only if we are in the same input group should we try and sort by
			// value (datetime)
			return key1.getDatetime().compareTo(key2.getDatetime());
			//return key2.getDatetime().compareTo(key1.getDatetime());
		}

		return compare;
	}
}
