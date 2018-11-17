package org.ccjmne.orca.api.config.providers;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
@Produces(MediaType.APPLICATION_JSON)
public class IllegalStateExceptionMapper implements ExceptionMapper<IllegalStateException> {

  private static final Logger LOGGER = LoggerFactory.getLogger(IllegalStateExceptionMapper.class);

  @Override
  public Response toResponse(final IllegalStateException e) {
    LOGGER.warn("Could not process request.", e);
    return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build();
  }
}
