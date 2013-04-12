package com.mongodb.hadoop.examples.wordcount;

import java.util.*;

/*
 * Jaccard Similarity is a similarity function which is calculated by 
 * first tokenizing the strings into sets and then taking the ratio of
 * (weighted) intersection to their union 
 */
public class JaccardSimilarity {

	public static double jaccardSimilarity(String similar1, String similar2) {
		HashSet<String> h1 = new HashSet<String>();
		HashSet<String> h2 = new HashSet<String>();
		
		for (String s : similar1.split("\\,+")) {
			h1.add(s);
		}
		System.out.println("h1 " + h1);
		for (String s : similar2.split("\\,+")) {
			h2.add(s);
		}
		System.out.println("h2 " + h2);

		int sizeh1 = h1.size();
		// Retains all elements in h3 that are contained in h2 ie intersection
		h1.retainAll(h2);
		// h1 now contains the intersection of h1 and h2
		System.out.println("Intersection " + h1);

		h2.removeAll(h1);
		// h2 now contains unique elements
		System.out.println("Unique in h2 " + h2);

		// Union
		int union = sizeh1 + h2.size();
		int intersection = h1.size();

		return (double) intersection / union;

	}

	public static void main(String args[]) {
		System.out.println(jaccardSimilarity(",153, West, Squire, Dr,",
				"147, West, Squire, Dr,"));

	}
}