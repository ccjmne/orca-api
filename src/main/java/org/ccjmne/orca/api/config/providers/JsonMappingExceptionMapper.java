package org.ccjmne.orca.api.config.providers;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonMappingException;

@Provider
@Produces(MediaType.APPLICATION_JSON)
public class JsonMappingExceptionMapper implements ExceptionMapper<JsonMappingException> {

	private static final Logger LOGGER = LoggerFactory.getLogger(JsonMappingExceptionMapper.class);

	@Override
	public Response toResponse(final JsonMappingException e) {
		LOGGER.warn("Could not process request.", e);
		return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build();
	}
}
