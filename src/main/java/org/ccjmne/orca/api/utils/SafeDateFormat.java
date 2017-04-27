package org.ccjmne.orca.api.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class SafeDateFormat {

	private static final String FORMAT = "yyyy-MM-dd";
	private static final ThreadLocal<DateFormat> delegate = new ThreadLocal<DateFormat>() {

		@Override
		protected DateFormat initialValue() {
			return new SimpleDateFormat(SafeDateFormat.FORMAT);
		}
	};

	public static java.sql.Date parseAsSql(final String source) throws ParseException {
		return new java.sql.Date(SafeDateFormat.delegate.get().parse(source).getTime());
	}

	public static DateFormat getDateFormat() {
		return SafeDateFormat.delegate.get();
	}
}
