package org.ccjmne.faomaintenance.api.utils;

import java.text.SimpleDateFormat;

@SuppressWarnings("serial")
public class SimplifiedDateFormat extends SimpleDateFormat {

	private static final String FORMAT = "yyyy-MM-dd";

	public SimplifiedDateFormat() {
		super(FORMAT);
	}
}
