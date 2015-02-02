package org.ccjmne.faomaintenance.api.rest;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.ccjmne.faomaintenance.api.jooq.Tables;
import org.ccjmne.faomaintenance.api.jooq.tables.records.SitesRecord;
import org.jooq.DSLContext;
import org.jooq.SelectQuery;

@Path("resources")
public class ResourceEndpoint {

	private final DSLContext ctx;

	@Inject
	public ResourceEndpoint(final DSLContext ctx) {
		this.ctx = ctx;
	}

	@GET
	@Path("employees")
	public List<Map<String, Object>> listEmployees() {
		return this.ctx.select().from(Tables.EMPLOYEES).fetch().intoMaps();
	}

	@GET
	@Path("employees/{registrationNumber}")
	public Map<String, Object> lookupEmpl(@PathParam("registrationNumber") final String registrationNumber) {
		return this.ctx.select().from(Tables.EMPLOYEES).where(Tables.EMPLOYEES.EMPL_PK.equal(registrationNumber)).fetchOne().intoMap();
	}

	@GET
	@Path("sites")
	public List<Map<String, Object>> listSites(@QueryParam("dept") final Integer dept) {
		final SelectQuery<SitesRecord> query = this.ctx.selectQuery(Tables.SITES);
		if (dept != null) {
			query.addConditions(Tables.SITES.SITE_DEPT_FK.eq(dept));
		}

		return query.fetchMaps();
	}

	@GET
	@Path("sites/{aurore}")
	public Map<String, Object> lookupSite(@PathParam("aurore") final String aurore) {
		return this.ctx.select().from(Tables.SITES).where(Tables.SITES.SITE_PK.equal(aurore)).fetchOne().intoMap();
	}

	@GET
	@Path("trainings")
	public List<Map<String, Object>> listTrainings() {
		return this.ctx.select().from(Tables.TRAININGS).fetch().intoMaps();
	}

	@GET
	@Path("trainings/{id}")
	public Map<String, Object> lookupTraining(@PathParam("id") final Integer id) {
		return this.ctx.select().from(Tables.TRAININGS).where(Tables.TRAININGS.TRNG_PK.equal(id)).fetchOne().intoMap();
	}

	@GET
	@Path("departments")
	public List<Map<String, Object>> listDepartments() {
		return this.ctx.select().from(Tables.DEPARTMENTS).fetch().intoMaps();
	}

	@GET
	@Path("training_types")
	public List<Map<String, Object>> listTrainingTypes() {
		return this.ctx.select().from(Tables.TRAININGTYPES).fetch().intoMaps();
	}

	@GET
	@Path("certificates")
	public List<Map<String, Object>> listCertificates() {
		return this.ctx.select().from(Tables.CERTIFICATES).fetch().intoMaps();
	}
}
