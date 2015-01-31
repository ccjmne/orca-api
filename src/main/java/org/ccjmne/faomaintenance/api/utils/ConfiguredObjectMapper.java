package org.ccjmne.faomaintenance.api.utils;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

@SuppressWarnings("serial")
public class ConfiguredObjectMapper extends ObjectMapper {

	public ConfiguredObjectMapper() {
		super();
		this.configure(SerializationFeature.INDENT_OUTPUT, true);
		this.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
		this.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

		setSerializationInclusion(Include.NON_EMPTY);
		this.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}
}
