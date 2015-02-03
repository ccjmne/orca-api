package org.ccjmne.faomaintenance.api.utils;

import java.io.IOException;
import java.util.Arrays;

import org.jooq.Record;
import org.jooq.Result;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

@SuppressWarnings("serial")
public class ConfiguredObjectMapper extends ObjectMapper {

	public ConfiguredObjectMapper() {
		this.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
		// TODO: clean up
		// this.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
		// this.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
		// false);

		registerModule(new JOOQResultsSerialiserModule());
	}

	private class JOOQResultsSerialiserModule extends SimpleModule {

		@SuppressWarnings("deprecation")
		public JOOQResultsSerialiserModule() {
			super("JOOQResultsSerialiserModule", new Version(1, 0, 0, null), Arrays.asList(new StdSerializer<Result<? extends Record>>(Result.class, false) {

				@Override
				public void serialize(final Result<? extends Record> value, final JsonGenerator jgen, final SerializerProvider provider) throws IOException,
						JsonGenerationException {
					jgen.writeObject(value.intoMaps());
				}
			}, new StdSerializer<Record>(Record.class, false) {

				@Override
				public void serialize(final Record value, final JsonGenerator jgen, final SerializerProvider provider) throws IOException,
						JsonGenerationException {
					jgen.writeObject(value.intoMap());
				}
			}));
		}
	}
}