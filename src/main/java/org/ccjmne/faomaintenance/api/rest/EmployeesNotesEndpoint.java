package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES_CERTIFICATES_OPTOUT;

import java.util.Collections;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.ccjmne.faomaintenance.api.modules.Restrictions;
import org.ccjmne.faomaintenance.api.modules.StatisticsCaches;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

@Path("employees-notes")
public class EmployeesNotesEndpoint {
	private final DSLContext ctx;
	private final StatisticsCaches statistics;

	@Inject
	public EmployeesNotesEndpoint(final DSLContext ctx, final StatisticsCaches statistics, final Restrictions restrictions) {
		if (!restrictions.canManageEmployeeNotes()) {
			throw new ForbiddenException();
		}

		this.ctx = ctx;
		this.statistics = statistics;
	}

	@PUT
	@Path("{empl_pk}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void setNotes(@PathParam("empl_pk") final String empl_pk, final Map<String, String> data) {
		this.ctx.update(EMPLOYEES).set(EMPLOYEES.EMPL_NOTES, data.get(EMPLOYEES.EMPL_NOTES.getName()))
				.where(EMPLOYEES.EMPL_PK.eq(empl_pk)).execute();
	}

	@POST
	@Path("{empl_pk}/optout")
	@Consumes(MediaType.APPLICATION_JSON)
	public void optOut(
						@PathParam("empl_pk") final String empl_pk,
						@QueryParam("cert_pk") final Integer cert_pk,
						@QueryParam("date") final java.sql.Date date,
						final Map<String, String> data) {
		if (this.ctx.fetchExists(DSL.selectFrom(EMPLOYEES_CERTIFICATES_OPTOUT).where(EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_EMPL_FK.eq(empl_pk))
				.and(EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_CERT_FK.eq(cert_pk)))) {
			this.ctx.update(EMPLOYEES_CERTIFICATES_OPTOUT)
					.set(EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_DATE, date)
					.set(EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_REASON, data.get(EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_REASON.getName()))
					.where(EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_EMPL_FK.eq(empl_pk)).and(EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_CERT_FK.eq(cert_pk))
					.execute();
		} else {
			this.ctx.insertInto(
								EMPLOYEES_CERTIFICATES_OPTOUT,
								EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_EMPL_FK,
								EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_CERT_FK,
								EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_DATE,
								EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_REASON)
					.values(empl_pk, cert_pk, date, data.get(EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_REASON.getName())).execute();
		}

		this.statistics.invalidateEmployeesStats(Collections.singletonList(empl_pk));
	}

	@DELETE
	@Path("{empl_pk}/optout")
	public void optBackIn(@PathParam("empl_pk") final String empl_pk, @QueryParam("cert_pk") final Integer cert_pk) {
		this.ctx.deleteFrom(EMPLOYEES_CERTIFICATES_OPTOUT).where(EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_EMPL_FK.eq(empl_pk))
				.and(EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_CERT_FK.eq(cert_pk)).execute();
		this.statistics.invalidateEmployeesStats(Collections.singletonList(empl_pk));
	}
}
