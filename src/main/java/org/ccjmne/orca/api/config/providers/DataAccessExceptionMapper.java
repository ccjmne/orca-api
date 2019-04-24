package org.ccjmne.orca.api.config.providers;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.jooq.exception.DataAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
@Produces(MediaType.APPLICATION_JSON)
public class DataAccessExceptionMapper implements ExceptionMapper<DataAccessException> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataAccessExceptionMapper.class);

  @Override
  public Response toResponse(final DataAccessException e) {
    LOGGER.warn("Could not process request.", e);
    return Response.status(Status.BAD_REQUEST).entity(e.getCause().getMessage()).build();
  }
}
