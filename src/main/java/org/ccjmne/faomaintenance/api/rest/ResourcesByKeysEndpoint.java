package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.DEPARTMENTS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS;

import java.text.ParseException;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import org.jooq.Record;

@Path("resources-by-keys")
// TODO: merge with ResourcesEndpoint?
public class ResourcesByKeysEndpoint {

	final ResourcesEndpoint resources;

	@Inject
	public ResourcesByKeysEndpoint(final ResourcesEndpoint resources) {
		this.resources = resources;
	}

	@GET
	@Path("employees")
	public Map<String, ? extends Record> listEmployees(
														@QueryParam("employee") final String empl_pk,
														@QueryParam("site") final String site_pk,
														@QueryParam("department") final Integer dept_pk,
														@QueryParam("training") final Integer trng_pk,
														@QueryParam("date") final String dateStr,
														@QueryParam("fields") final String fields) {
		return this.resources.listEmployees(empl_pk, site_pk, dept_pk, trng_pk, dateStr, fields).intoMap(EMPLOYEES.EMPL_PK);
	}

	@GET
	@Path("sites")
	public Map<String, ? extends Record> listSites(
													@QueryParam("site") final String site_pk,
													@QueryParam("department") final Integer dept_pk,
													@QueryParam("date") final String dateStr,
													@QueryParam("unlisted") final boolean unlisted) {
		return this.resources.listSites(site_pk, dept_pk, dateStr, unlisted).intoMap(SITES.SITE_PK);
	}

	@GET
	@Path("departments")
	public Map<Integer, ? extends Record> listDepartments(
															@QueryParam("department") final Integer dept_pk,
															@QueryParam("date") final String dateStr,
															@QueryParam("unlisted") final boolean unlisted) {
		return this.resources.listDepartments(dept_pk, dateStr, unlisted).intoMap(DEPARTMENTS.DEPT_PK);
	}

	@GET
	@Path("trainings")
	public Map<Integer, Record> listTrainings(
												@QueryParam("employee") final String empl_pk,
												@QueryParam("type") final List<Integer> types,
												@QueryParam("date") final String dateStr,
												@QueryParam("from") final String fromStr,
												@QueryParam("to") final String toStr,
												@QueryParam("completed") final Boolean completedOnly)
			throws ParseException {
		return this.resources.listTrainings(empl_pk, types, dateStr, fromStr, toStr, completedOnly).intoMap(TRAININGS.TRNG_PK);
	}
}
