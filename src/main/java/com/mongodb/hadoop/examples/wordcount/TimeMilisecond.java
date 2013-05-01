package com.mongodb.hadoop.examples.wordcount;

import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang.time.DateFormatUtils;

public class TimeMilisecond {
	public static void main(String[] argv) {
		 
	      long lDateTime = new Date().getTime();
	      System.out.println("Date() - Time in milliseconds: " + lDateTime);
	 
	      Calendar lCDateTime = Calendar.getInstance();
	      System.out.println("Calender - Time in milliseconds :" + lCDateTime.getTimeInMillis());
	   // Format Date into dd-MM-yyyy
	      System.out.println("1) dd-MM-yyyy >>>" + DateFormatUtils.format(new Date(), "dd-MM-yyyy"));

	      // Format Date into SMTP_DATETIME_FORMAT
	      System.out.println("2) SMTP_DATETIME_FORMAT >>>"
	          + DateFormatUtils.SMTP_DATETIME_FORMAT.format(new Date()));

	      // Format Date into ISO_DATE_FORMAT
	      System.out.println("3) ISO_DATE_FORMAT >>>"
	          + DateFormatUtils.ISO_DATE_FORMAT.format(new Date()));

	      // Format milliseconds in long
	      System.out.println("4) MMM dd yy HH:mm >>>"
	          + DateFormatUtils.format(System.currentTimeMillis(), "MMM dd yy HH:mm"));

	      // Format milliseconds in long using UTC timezone
	      System.out.println("5) MM/dd/yy HH:mm >>>"
	          + DateFormatUtils.formatUTC(System.currentTimeMillis(), "MM/dd/yy HH:mm"));
	  }
}
