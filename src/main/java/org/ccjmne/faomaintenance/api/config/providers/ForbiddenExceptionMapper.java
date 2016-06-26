package org.ccjmne.faomaintenance.api.config.providers;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.ccjmne.faomaintenance.api.utils.ForbiddenException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
@Produces(MediaType.APPLICATION_JSON)
public class ForbiddenExceptionMapper implements ExceptionMapper<ForbiddenException> {

	private static final Logger LOGGER = LoggerFactory.getLogger(ForbiddenExceptionMapper.class);

	@Override
	public Response toResponse(final ForbiddenException e) {
		LOGGER.warn("Unauthorized API call received.", e);
		return Response.status(Status.FORBIDDEN).entity(e.getMessage()).build();
	}
}
