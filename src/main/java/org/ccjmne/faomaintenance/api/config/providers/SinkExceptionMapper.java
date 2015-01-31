package org.ccjmne.faomaintenance.api.config.providers;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
@Produces(MediaType.APPLICATION_JSON)
public class SinkExceptionMapper implements ExceptionMapper<Exception> {

	@Override
	public Response toResponse(final Exception e) {
		e.printStackTrace();
		return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build();
	}
}
