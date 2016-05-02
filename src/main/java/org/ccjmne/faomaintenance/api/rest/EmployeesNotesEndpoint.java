package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES_CERTIFICATES_OPTOUT;

import java.util.Collections;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.ccjmne.faomaintenance.jooq.classes.Tables;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

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

	@Path("{empl_pk}/optout")
	@POST
	public void optOut(@PathParam("empl_pk") final String empl_pk, @QueryParam("cert_pk") final Integer cert_pk, @QueryParam("date") final java.sql.Date date) {
		if (this.ctx.fetchExists(DSL.selectFrom(EMPLOYEES_CERTIFICATES_OPTOUT).where(EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_EMPL_FK.eq(empl_pk))
				.and(EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_CERT_FK.eq(cert_pk)))) {
			this.ctx.update(EMPLOYEES_CERTIFICATES_OPTOUT).set(EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_DATE, date)
					.where(EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_EMPL_FK.eq(empl_pk)).and(EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_CERT_FK.eq(cert_pk))
					.execute();
		} else {
			this.ctx.insertInto(
								Tables.EMPLOYEES_CERTIFICATES_OPTOUT,
								Tables.EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_EMPL_FK,
								Tables.EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_CERT_FK,
								Tables.EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_DATE)
					.values(empl_pk, cert_pk, date).execute();
		}

		this.statisticsEndpoint.invalidateEmployeesStats(Collections.singletonList(empl_pk));
	}

	@Path("{empl_pk}/optout")
	@DELETE
	public void optBackIn(@PathParam("empl_pk") final String empl_pk, @QueryParam("cert_pk") final Integer cert_pk) {
		this.ctx.deleteFrom(EMPLOYEES_CERTIFICATES_OPTOUT).where(EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_EMPL_FK.eq(empl_pk))
				.and(EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_CERT_FK.eq(cert_pk)).execute();
		this.statisticsEndpoint.invalidateEmployeesStats(Collections.singletonList(empl_pk));
	}
}
