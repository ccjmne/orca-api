package org.ccjmne.faomaintenance.api.rest;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import org.ccjmne.faomaintenance.api.db.DBClient;
import org.ccjmne.faomaintenance.api.resources.Training;

@Path("trainings")
public class TrainingsEndpoint {

	private final DBClient dbClient;
	private final DateFormat dateFormat;

	@Inject
	public TrainingsEndpoint(final DBClient dbClient, final DateFormat dateFormat) {
		this.dbClient = dbClient;
		this.dateFormat = dateFormat;
	}

	@GET
	@Path("{trainingId}")
	public Training lookup(@PathParam("trainingId") final int trainingId) {
		return this.dbClient.lookupTraining(trainingId);
	}

	@GET
	public Response list(@QueryParam("from") final String from, @QueryParam("to") final String to, @QueryParam("types") final List<Integer> types) {
		try {
			final List<Training> trainings;
			if (to == null) {
				trainings = this.dbClient.listTrainings(this.dateFormat.parse(from), types.toArray(new Integer[0]));
			} else {
				trainings = this.dbClient.listTrainings(this.dateFormat.parse(from), this.dateFormat.parse(to), types.toArray(new Integer[0]));
			}

			return Response.ok(trainings).build();
		} catch (final ParseException e) {
			return Response.status(Status.BAD_REQUEST).build();
		}
	}

	@POST
	public Response post(@Context final UriInfo uriInfo, final Map<String, Object> map) {
		return Response.created(uriInfo.getBaseUri().resolve("trainings/" + this.dbClient.addTraining(map))).build();
	}

	@PUT
	@Path("{trainingId}")
	public Response put(@Context final UriInfo uriInfo, @PathParam("trainingId") final int trainingId, final Map<String, Object> map) {
		if (this.dbClient.updateTraining(trainingId, map)) {
			return Response.accepted(uriInfo.getAbsolutePath()).build();
		}

		return Response.created(uriInfo.getAbsolutePath()).build();
	}

	@DELETE
	@Path("{trainingId}")
	public Response delete(@PathParam("trainingId") final int trainingId) {
		if (this.dbClient.deleteTraining(trainingId)) {
			return Response.accepted().build();
		}

		return Response.notModified().build();
	}
}
