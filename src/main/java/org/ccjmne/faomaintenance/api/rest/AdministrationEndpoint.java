package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES_ROLES;

import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;

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
		return this.ctx
				.select(
						EMPLOYEES.EMPL_PK,
						EMPLOYEES.EMPL_FIRSTNAME,
						EMPLOYEES.EMPL_SURNAME,
						EMPLOYEES.EMPL_GENDER,
						DSL.arrayAgg(EMPLOYEES_ROLES.EMRO_TYPE).as("roles"))
				.from(EMPLOYEES).join(EMPLOYEES_ROLES).on(EMPLOYEES_ROLES.EMPL_PK.eq(EMPLOYEES.EMPL_PK)).where(EMPLOYEES.EMPL_PK.eq(empl_pk))
				.groupBy(EMPLOYEES.EMPL_PK, EMPLOYEES.EMPL_FIRSTNAME, EMPLOYEES.EMPL_SURNAME, EMPLOYEES.EMPL_GENDER).fetchOneMap();
	}
}
