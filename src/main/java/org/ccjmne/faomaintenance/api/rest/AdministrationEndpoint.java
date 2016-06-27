package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES_ROLES;

import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import org.ccjmne.faomaintenance.api.modules.Restrictions;
import org.ccjmne.faomaintenance.api.utils.Constants;
import org.jooq.DSLContext;

@Path("admin")
public class AdministrationEndpoint {

	private final DSLContext ctx;

	@Inject
	public AdministrationEndpoint(final DSLContext ctx) {
		this.ctx = ctx;
	}

	@GET
	@Path("users/{empl_pk}")
	public Map<String, Object> getUserInfo(@PathParam("empl_pk") final String empl_pk) {
		final Map<String, Object> res = this.ctx
				.select(
						EMPLOYEES.EMPL_PK,
						EMPLOYEES.EMPL_FIRSTNAME,
						EMPLOYEES.EMPL_SURNAME,
						EMPLOYEES.EMPL_DOB,
						EMPLOYEES.EMPL_PERMANENT,
						EMPLOYEES.EMPL_GENDER,
						EMPLOYEES.EMPL_NOTES,
						EMPLOYEES.EMPL_ADDR)
				.from(EMPLOYEES).where(EMPLOYEES.EMPL_PK.eq(empl_pk))
				.fetchOneMap();
		res.put(
				"roles",
				this.ctx.selectFrom(EMPLOYEES_ROLES).where(EMPLOYEES_ROLES.EMPL_PK.eq(empl_pk))
						.fetchMap(EMPLOYEES_ROLES.EMRO_TYPE, Constants.EMPLOYEES_ROLES_MAPPER));
		res.put("restrictions", Restrictions.forEmployee(empl_pk, this.ctx));
		return res;
	}
}
