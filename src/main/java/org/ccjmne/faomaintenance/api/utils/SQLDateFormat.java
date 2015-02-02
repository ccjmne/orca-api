package org.ccjmne.faomaintenance.api.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;

@SuppressWarnings("serial")
public class SQLDateFormat extends SimpleDateFormat {

	private static final String FORMAT = "yyyy-MM-dd";

	public SQLDateFormat() {
		super(FORMAT);
	}

	public java.sql.Date parseSql(final String source) throws ParseException {
		return new java.sql.Date(this.parse(source).getTime());
	}
}
