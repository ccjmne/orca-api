package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;

import java.util.Collections;
import java.util.Date;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;

import org.jooq.DSLContext;

@Path("employees-notes")
public class EmployeesNotesEndpoint {
	private final DSLContext ctx;
	private final StatisticsEndpoint statisticsEndpoint;

	@Inject
	public EmployeesNotesEndpoint(final DSLContext ctx, final StatisticsEndpoint statisticsEndpoint) {
		this.ctx = ctx;
		this.statisticsEndpoint = statisticsEndpoint;
	}

	@Path("{empl_pk}")
	@PUT
	@Consumes(MediaType.APPLICATION_JSON)
	public void setNotes(@PathParam("empl_pk") final String empl_pk, final Map<String, String> data) {
		this.ctx.update(EMPLOYEES).set(EMPLOYEES.EMPL_NOTES, data.get(EMPLOYEES.EMPL_NOTES.getName()))
				.where(EMPLOYEES.EMPL_PK.eq(empl_pk)).execute();
	}

	@Path("{empl_pk}/sst-optout")
	@POST
	public void optOut(@PathParam("empl_pk") final String empl_pk) {
		this.ctx.update(EMPLOYEES).set(EMPLOYEES.EMPL_SST_OPTOUT, new java.sql.Date(new Date().getTime()))
				.where(EMPLOYEES.EMPL_PK.eq(empl_pk)).execute();
		this.statisticsEndpoint.invalidateEmployeesStats(Collections.singletonList(empl_pk));
	}

	@Path("{empl_pk}/sst-optout")
	@DELETE
	public void optBackIn(@PathParam("empl_pk") final String empl_pk) {
		this.ctx.update(EMPLOYEES).set(EMPLOYEES.EMPL_SST_OPTOUT, (java.sql.Date) null)
				.where(EMPLOYEES.EMPL_PK.eq(empl_pk)).execute();
		this.statisticsEndpoint.invalidateEmployeesStats(Collections.singletonList(empl_pk));
	}
}
