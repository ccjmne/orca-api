package org.ccjmne.faomaintenance.api.config.providers;

import java.util.NoSuchElementException;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.ccjmne.faomaintenance.api.rest.ResponseEntity;

@Provider
@Produces(MediaType.APPLICATION_JSON)
public class NoSuchElementExceptionMapper implements ExceptionMapper<NoSuchElementException> {

	@Override
	public Response toResponse(final NoSuchElementException exception) {
		return Response.status(Status.NOT_FOUND).entity(ResponseEntity.error("BAD_REFERENCE", exception.getMessage())).build();
	}
}
