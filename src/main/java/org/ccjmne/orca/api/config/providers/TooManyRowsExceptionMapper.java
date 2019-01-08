package org.ccjmne.orca.api.config.providers;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.jooq.exception.TooManyRowsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
@Produces(MediaType.APPLICATION_JSON)
public class TooManyRowsExceptionMapper implements ExceptionMapper<TooManyRowsException> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TooManyRowsExceptionMapper.class);

  @Override
  public Response toResponse(final TooManyRowsException e) {
    LOGGER.warn("Could not process request.", e);
    return Response.status(Status.NOT_FOUND).entity(e.getMessage()).build();
  }
}
