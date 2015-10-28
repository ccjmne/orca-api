package org.ccjmne.faomaintenance.api.config.providers;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
public class NumberFormatExceptionMapper implements ExceptionMapper<NumberFormatException> {

	private static final Logger LOGGER = LoggerFactory.getLogger(ParseExceptionMapper.class);

	@Override
	public Response toResponse(final NumberFormatException e) {
		LOGGER.warn("Could not process request.", e);
		return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build();
	}
}
