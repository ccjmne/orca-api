package org.ccjmne.orca.api.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class SafeDateFormat {

	private static final String FORMAT = "yyyy-MM-dd";
	private static final ThreadLocal<DateFormat> delegate = new ThreadLocal<DateFormat>() {

		@SuppressWarnings("serial")
		@Override
		protected DateFormat initialValue() {
			return new SimpleDateFormat(SafeDateFormat.FORMAT) {

				@Override
				public StringBuffer format(final java.util.Date date, final StringBuffer toAppendTo, final java.text.FieldPosition pos) {
					if (date.equals(Constants.DATE_INFINITY)) {
						return toAppendTo.insert(pos.getBeginIndex(), Constants.DATE_INFINITY_LITERAL);
					}

					return super.format(date, toAppendTo, pos);
				}
			};
		}
	};

	public static java.sql.Date parseAsSql(final String source) throws ParseException {
		return new java.sql.Date(SafeDateFormat.delegate.get().parse(source).getTime());
	}

	public static DateFormat getDateFormat() {
		return SafeDateFormat.delegate.get();
	}
}
