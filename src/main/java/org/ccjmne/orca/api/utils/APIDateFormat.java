package org.ccjmne.orca.api.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class APIDateFormat {

  public static final String FORMAT = "yyyy-MM-dd";

  private static final ThreadLocal<DateFormat> DELEGATE = new ThreadLocal<DateFormat>() {

    @Override
    @SuppressWarnings("serial")
    protected DateFormat initialValue() {
      return new SimpleDateFormat(APIDateFormat.FORMAT) {

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
    return new java.sql.Date(APIDateFormat.DELEGATE.get().parse(source).getTime());
  }

  public static DateFormat getDateFormat() {
    return APIDateFormat.DELEGATE.get();
  }
}
