package org.ccjmne.faomaintenance.api.config.providers;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.ccjmne.faomaintenance.api.utils.UnauthorizedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
@Produces(MediaType.APPLICATION_JSON)
public class UnauthorizedExceptionMapper implements ExceptionMapper<UnauthorizedException> {

	private static final Logger LOGGER = LoggerFactory.getLogger(UnauthorizedExceptionMapper.class);

	@Override
	public Response toResponse(final UnauthorizedException e) {
		LOGGER.warn("Unauthorized API call received.", e);
		return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build();
	}
}
