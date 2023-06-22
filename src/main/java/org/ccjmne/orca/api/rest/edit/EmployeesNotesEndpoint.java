package org.ccjmne.orca.api.rest.edit;

import static org.ccjmne.orca.jooq.codegen.Tables.EMPLOYEES;
import static org.ccjmne.orca.jooq.codegen.Tables.EMPLOYEES_VOIDINGS;

import java.time.LocalDate;
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

import org.ccjmne.orca.api.modules.Restrictions;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

@Path("employees-notes")
public class EmployeesNotesEndpoint {

	private final DSLContext ctx;

	@Inject
	public EmployeesNotesEndpoint(final DSLContext ctx, final Restrictions restrictions) {
		if (!restrictions.canManageEmployeeNotes()) {
			throw new ForbiddenException();
		}

		this.ctx = ctx;
	}

	@PUT
	@Path("{empl_pk}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void setNotes(@PathParam("empl_pk") final Integer empl_pk, final Map<String, String> data) {
		this.ctx.update(EMPLOYEES).set(EMPLOYEES.EMPL_NOTES, data.get(EMPLOYEES.EMPL_NOTES.getName()))
				.where(EMPLOYEES.EMPL_PK.eq(empl_pk)).execute();
	}

	@POST
	@Path("{empl_pk}/voiding")
	@Consumes(MediaType.APPLICATION_JSON)
	public void optOut(
						@PathParam("empl_pk") final Integer empl_pk,
						@QueryParam("cert_pk") final Integer cert_pk,
						@QueryParam("date") final LocalDate date,
						final Map<String, String> data) {
		if (this.ctx.fetchExists(DSL.selectFrom(EMPLOYEES_VOIDINGS).where(EMPLOYEES_VOIDINGS.EMVO_EMPL_FK.eq(empl_pk))
				.and(EMPLOYEES_VOIDINGS.EMVO_CERT_FK.eq(cert_pk)))) {
			this.ctx.update(EMPLOYEES_VOIDINGS)
					.set(EMPLOYEES_VOIDINGS.EMVO_DATE, date)
					.set(EMPLOYEES_VOIDINGS.EMVO_REASON, data.get(EMPLOYEES_VOIDINGS.EMVO_REASON.getName()))
					.where(EMPLOYEES_VOIDINGS.EMVO_EMPL_FK.eq(empl_pk)).and(EMPLOYEES_VOIDINGS.EMVO_CERT_FK.eq(cert_pk))
					.execute();
		} else {
			this.ctx.insertInto(
								EMPLOYEES_VOIDINGS,
								EMPLOYEES_VOIDINGS.EMVO_EMPL_FK,
								EMPLOYEES_VOIDINGS.EMVO_CERT_FK,
								EMPLOYEES_VOIDINGS.EMVO_DATE,
								EMPLOYEES_VOIDINGS.EMVO_REASON)
					.values(empl_pk, cert_pk, date, data.get(EMPLOYEES_VOIDINGS.EMVO_REASON.getName())).execute();
		}
	}

	@DELETE
	@Path("{empl_pk}/voiding")
	public void optBackIn(@PathParam("empl_pk") final Integer empl_pk, @QueryParam("cert_pk") final Integer cert_pk) {
		this.ctx.deleteFrom(EMPLOYEES_VOIDINGS).where(EMPLOYEES_VOIDINGS.EMVO_EMPL_FK.eq(empl_pk))
				.and(EMPLOYEES_VOIDINGS.EMVO_CERT_FK.eq(cert_pk)).execute();
	}
}
