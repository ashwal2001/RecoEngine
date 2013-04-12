package com.mongodb.hadoop.examples.wordcount;

public class PearsonCorrelationTest {

	public double computeAllPairwiseCorrelations(double[][] data) {
		double corr = 0;
		int numCols = data[0].length;
		for (int i = 0; i < numCols; i++) {
			for (int j = i + 1; j < numCols; j++) {
				corr = computeCorrelation(i, j, data); // compute the
														// correlation
				// between the i-th and j-th
				// columns/variables
			}
		}
		return corr;
	}

	public double computeCorrelation(int i, int j, double[][] data) {
		double x = 0, y = 0, xx = 0, yy = 0, xy = 0, n = 0;
		n = data.length;
		for (int row = 0; row < data.length; row++) {
			x += data[row][i];
			y += data[row][j];
			xx += Math.pow(data[row][i], 2.0d);
			yy += Math.pow(data[row][j], 2.0d);
			xy += data[row][i] * data[row][j];
		}
		double numerator = xy - ((x * y) / n);
		double denominator1 = xx - (Math.pow(x, 2.0d) / n);
		double denominator2 = yy - (Math.pow(y, 2.0d) / n);
		double denominator = Math.sqrt(xx * yy);
		double corr = numerator / denominator;
		return corr;
	}

	public static void main(String[] args) {
		double[][] data = { { 1, 1, 3, -1 }, { 2, 2, 1, -2 }, { 3, 3, 8, -3 } };
		PearsonCorrelationTest test = new PearsonCorrelationTest();
		double corr = test.computeAllPairwiseCorrelations(data);
		System.out.println(corr);
	}
}
