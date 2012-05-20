package org.hadoop.sbu.util;

import java.util.Random;

public class RandomGenerator {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Random r = new Random();
		double a = Math.pow(10,-8);
		double b = Math.pow(10,-9);
		for(int i=0;i<10;i++)	{
			System.out.println("1: "+(a+ r.nextDouble()*(b-a)));
			System.out.println("2: "+(Math.random()*a));
		}
	}

}
