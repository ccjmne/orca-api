package org.ccjmne.orca.api.config.providers;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.Provider;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;

@Provider
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class ConfiguredJacksonJaxbJsonProvider extends JacksonJaxbJsonProvider {

  @Inject
  public ConfiguredJacksonJaxbJsonProvider(final ObjectMapper mapper) {
    super(mapper, null);
  }
}
