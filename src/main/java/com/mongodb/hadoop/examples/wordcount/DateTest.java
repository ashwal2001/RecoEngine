package com.mongodb.hadoop.examples.wordcount;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;

public class DateTest {
	
	public static void main(String[] args) {
		SimpleDateFormat format = 
			    new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
			format.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "IST")));
			try {
				Date date = format.parse("2012-01-20T00:00:00.000Z");
				System.out.println(date);
				format = 
					    new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
				date = format.parse("2013-02-26T13:44:56Z");
				System.out.println(date);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}

}
