package org.ccjmne.orca.api.utils;

import java.io.IOException;
import java.util.Arrays;

import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.Result;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.google.common.collect.ImmutableMap;

@SuppressWarnings("serial")
public class CustomObjectMapper extends ObjectMapper {

  public CustomObjectMapper() {
    super.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    super.registerModule(new JavaTimeModule());
    super.registerModule(new JOOQResultsSerialiserModule());
    super.registerModule(new AfterburnerModule());
    super.registerModule(new JooqJSONBSerialiserModule());
  }

  private class JOOQResultsSerialiserModule extends SimpleModule {

    @SuppressWarnings("null")
    public JOOQResultsSerialiserModule() {
      super(
            "JOOQResultsSerialiserModule",
            new Version(1, 0, 0, null, null, null),
            Arrays.asList(new StdSerializer<Result<? extends Record>>(Result.class, false) {

              @Override
              public void serialize(final Result<? extends Record> value, final JsonGenerator jgen, final SerializerProvider provider)
                  throws IOException, JsonGenerationException {
                jgen.writeObject(value.intoMaps());
              }
            }, new StdSerializer<Record>(Record.class, false) {

              @Override
              public void serialize(final Record value, final JsonGenerator jgen, final SerializerProvider provider)
                  throws IOException, JsonGenerationException {
                jgen.writeObject(value.intoMap());
              }
            }));
    }
  }

  private class JooqJSONBSerialiserModule extends SimpleModule {

    @SuppressWarnings("null")
    public JooqJSONBSerialiserModule() {
      super(
            "JooqJSONBSerialiserModule",
            new Version(1, 0, 0, null, null, null),
            ImmutableMap.of(JSONB.class, new StdDeserializer<JSONB>(JSONB.class) {

              @Override
              public JSONB deserialize(final JsonParser jp, final DeserializationContext ctxt) throws IOException, JsonProcessingException {
                return JSONB.valueOf(jp.readValueAsTree().toString());
              }
            }), Arrays.asList(new StdSerializer<JSONB>(JSONB.class, false) {

              @Override
              public void serialize(final JSONB value, final JsonGenerator jgen, final SerializerProvider provider) throws IOException {
                jgen.writeRawValue(value.data());
              }
            }));
    }
  }
}
