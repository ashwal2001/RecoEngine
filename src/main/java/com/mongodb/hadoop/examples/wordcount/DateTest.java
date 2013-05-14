package com.mongodb.hadoop.examples.wordcount;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;

public class DateTest {

	public static void main(String[] args) {
		SimpleDateFormat format = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		format.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "IST")));
		try {
			Date date = format.parse("2012-01-20T00:00:00.000Z");
			System.out.println(date);
			date = format.parse("2013-02-26T13:44:56.940Z");
			System.out.println(date);
			format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
			format.setCalendar(new GregorianCalendar(new SimpleTimeZone(0,
					"IST")));
			date = format.parse("2013-02-26T13:44:56Z");
			System.out.println(date);
			System.out.println(getDate("2013-02-26T13:44:56Z"));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static String getDate(String dateStr) {
		SimpleDateFormat format = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss'Z'");
		format.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "IST")));
		Date date = null;
		try {
			date = format.parse(dateStr);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date.toString();
	}

}
