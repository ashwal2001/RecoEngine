package com.xyz.reccommendation.jaccard;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class CPair implements WritableComparable {
	private String x;
	private int count;

	// required methods
	public CPair() {
		x = "";
		count = 0;
	}

	public String toString() {
		return x + " " + count;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(x);
		// "/n" needed here so that the readLine() calls in readFields will
		// read the correct input
		out.writeInt(count);
	}

	public void readFields(DataInput in) throws IOException {
		x = in.readUTF();
		count = in.readInt();
	}

	public static CPair genc(StringTokenizer st) {
		return new CPair(st.nextToken(), Integer.parseInt(st.nextToken()));
	}

	// real methods
	public CPair(String a, int b) {
		x = a;
		count = b;
	}

	public int hashCode() {
		return x.hashCode() * 127 + new Integer(count).hashCode();
	}

	public int compareTo(Object q) {
		CPair p = (CPair) q;
		return getX().compareTo(p.getX());
	}

	public boolean equals(CPair p) {
		return compareTo(p) == 0;
	}

	public String getX() {
		return x;
	}

	public int getCount() {
		return count;
	}
}
