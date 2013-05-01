/*
 * ===========================================================================
 * EnvtTest.java
 *
 * Created on 01-May-2013
 *
 * This code contains copyright information which is the proprietary property
 * of Jade eServices. No part of this code may be reproduced, stored or transmitted
 * in any form without the prior written permission of Jade eServices.
 *
 * Copyright (C) Jade eServices. 2013
 * All rights reserved.
 *
 * Modification history:
 * $Log: EnvtTest.java,v $
 * ===========================================================================
 */
package com.xyz.reccommendation.jaccard;

/**
 * java -cp target/RecoEngine-1.0-SNAPSHOT.jar -DenvtType=hello
 * com.xyz.reccommendation.jaccard.EnvtTest -DenvtType="hello world"
 * 
 * @author ashok
 * 
 * @version $Id: EnvtTest.java,v 1.1 01-May-2013 11:39:42 AM ashok Exp $
 */
public class EnvtTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String envt = System.getProperty("envtType");
		System.out.println("###" + envt);
		System.out.println(args.length);
		System.out.println(args[0]);
	}

}
