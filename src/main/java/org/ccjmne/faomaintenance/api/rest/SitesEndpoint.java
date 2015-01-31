package org.ccjmne.faomaintenance.api.rest;

import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.ccjmne.faomaintenance.api.db.DBClient;
import org.ccjmne.faomaintenance.api.resources.Site;

@Path("sites")
public class SitesEndpoint {

	private final DBClient dbClient;

	@Inject
	public SitesEndpoint(final DBClient dbClient) {
		this.dbClient = dbClient;
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	/**
	 * List all Sites' aurore code, name and coordinates
	 */
	public List<Site> list() {
		return this.dbClient.listSites();
	}

	@GET
	@Path("{aurore}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response get(@PathParam("aurore") final String aurore) {
		final Site res;
		if ((res = this.dbClient.lookupSite(aurore)) == null) {
			return Response.status(Status.NOT_FOUND).build();
		}

		return Response.status(Status.OK).entity(res).build();
	}

	@GET
	@Path("{aurore}/employees")
	@Produces(MediaType.APPLICATION_JSON)
	public Response listEmployees(@PathParam("aurore") final String aurore) {
		return Response.status(Status.OK).entity(this.dbClient.listEmployees(aurore)).build();
	}
}
