package org.ccjmne.faomaintenance.api.rest;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import org.ccjmne.faomaintenance.api.db.DBClient;
import org.ccjmne.faomaintenance.api.resources.Employee;

@Path("employees")
public class EmployeesEndpoint {

	private final DBClient dbClient;

	@Inject
	public EmployeesEndpoint(final DBClient dbClient) {
		this.dbClient = dbClient;
	}

	@GET
	@Path("{registrationNumber}")
	@Produces(MediaType.APPLICATION_JSON)
	public Employee get(final @PathParam("registrationNumber") String registrationNumber) {
		return this.dbClient.lookupEmployee(registrationNumber);
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response post(@Context final UriInfo uriInfo, final Employee employee) {
		if (!this.dbClient.addEmployee(employee)) {
			return Response.status(Status.BAD_REQUEST).build();
		}

		return Response.created(uriInfo.getBaseUri().resolve("employees/" + employee.registrationNumber)).build();
	}

	@PUT
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Path("{registrationNumber}")
	public Response put(final @PathParam("registrationNumber") String registrationNumber, @Context final UriInfo uriInfo, final Employee employee) {
		if (!this.dbClient.updateEmployee(registrationNumber, employee)) {
			return Response.status(Status.BAD_REQUEST).build();
		}

		return Response.accepted().location(uriInfo.getAbsolutePath()).build();
	}
}
