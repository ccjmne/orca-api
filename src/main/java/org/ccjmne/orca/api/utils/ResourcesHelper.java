package org.ccjmne.orca.api.utils;

public class ResourcesHelper {

	public static Object tagValueCoercer(final String type, final String value) {
		return Constants.TAGS_TYPE_BOOLEAN.equals(type) ? Boolean.valueOf(value) : value;
	}
}
