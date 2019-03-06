package org.ccjmne.orca.api.config.providers;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.jooq.exception.NoDataFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
@Produces(MediaType.APPLICATION_JSON)
public class NoDataFoundExceptionMapper implements ExceptionMapper<NoDataFoundException> {

  private static final Logger LOGGER = LoggerFactory.getLogger(NoDataFoundExceptionMapper.class);

  @Override
  public Response toResponse(final NoDataFoundException e) {
    LOGGER.warn("Could not process request.", e);
    return Response.status(Status.NOT_FOUND).entity(e.getMessage()).build();
  }
}
