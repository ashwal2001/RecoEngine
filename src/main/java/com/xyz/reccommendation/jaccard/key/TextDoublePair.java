package com.xyz.reccommendation.jaccard.key;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextDoublePair implements WritableComparable<TextDoublePair> {

	public Text first;
	public DoubleWritable second;

	/**
	 * @return the first
	 */
	public Text getFirst() {
		return first;
	}

	/**
	 * @param first
	 *            the first to set
	 */
	public void setFirst(Text first) {
		this.first = first;
	}

	/**
	 * @return the second
	 */
	public DoubleWritable getSecond() {
		return second;
	}

	/**
	 * @param second
	 *            the second to set
	 */
	public void setSecond(DoubleWritable second) {
		this.second = second;
	}

	public TextDoublePair() {
		first = new Text();
		second = new DoubleWritable();
	}

	public void set(String first, double second) {
		this.first.set(first);
		this.second.set(second);
	}

	public TextDoublePair(Text first, DoubleWritable second) {
		super();
		this.first = first;
		this.second = second;
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		first.readFields(dataInput);
		second.readFields(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		first.write(dataOutput);
		second.write(dataOutput);
	}

	// @Override
	// public int hashCode() {
	// return first.hashCode() * 163 + second.hashCode();
	// }

	@Override
	public int compareTo(TextDoublePair textDoublePair) {
		int cmp = first.compareTo(textDoublePair.first);
		if (cmp != 0) {
			return cmp;
		}
		return second.compareTo(textDoublePair.second);
	}

	@Override
	public String toString() {
		return first + "\t" + second;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + ((second == null) ? 0 : second.hashCode());
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null || getClass() != obj.getClass())
			return false;
		TextDoublePair other = (TextDoublePair) obj;
		if (first == null) {
			if (other.first != null)
				return false;
		} else if (!first.equals(other.first))
			return false;
		if (second == null) {
			if (other.second != null)
				return false;
		} else if (!second.equals(other.second))
			return false;
		return true;
	}

}
