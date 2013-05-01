package com.mongodb.hadoop.examples.wordcount;

public class PairTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String str = "NI091MA32QWTINDFAS, NI091MA28QWXINDFAS, NI091MA44QWHINDFAS, UN573MA24POLINDFAS, ";
		String[] array = str.split(", ");
//		System.out.println(array.length);
//		for (int i = 0; i < array.length - 1; i++)
//			System.out.println(array[i] + " : " + array[i + 1]);
//		
//		System.out.println();
//		
//		for (int x = 0; x < array.length-1; x++) {
//			for (int y = x+1; y < array.length; y++)
//				System.out.println(array[x] + " : " + array[y]);
//		}
		
		str = ", NI091MA32QWTINDFAS, NI091MA28QWXINDFAS, NI091MA44QWHINDFAS, UN573MA24POLINDFAS";
		array = str.split(", ");
		System.out.println(array.length);
		for (int i = 1; i < array.length - 1; i++)
			System.out.println(array[i] + " : " + array[i + 1]);
		
		str = str.replaceFirst(", ", "");
		array = str.split(", ");
		System.out.println(array.length);
		for (int i = 0; i < array.length - 1; i++)
			System.out.println(array[i] + " : " + array[i + 1]);
	}

}
