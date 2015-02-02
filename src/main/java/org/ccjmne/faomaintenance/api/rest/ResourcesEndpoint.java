package org.ccjmne.faomaintenance.api.rest;

import java.text.ParseException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.ccjmne.faomaintenance.api.jooq.Tables;
import org.ccjmne.faomaintenance.api.jooq.tables.records.SitesRecord;
import org.ccjmne.faomaintenance.api.utils.NoDataException;
import org.ccjmne.faomaintenance.api.utils.SQLDateFormat;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.SelectQuery;
import org.jooq.exception.DataAccessException;
import org.jooq.exception.InvalidResultException;

@Path("resources")
public class ResourcesEndpoint {

	private final DSLContext ctx;
	private final SQLDateFormat dateFormat;

	@Inject
	public ResourcesEndpoint(final DSLContext ctx, final SQLDateFormat dateFormat) {
		this.ctx = ctx;
		this.dateFormat = dateFormat;
	}

	@GET
	@Path("employees")
	public List<Map<String, Object>> listEmployees(@QueryParam("site") final String aurore, @QueryParam("date") final String dateStr)
			throws InvalidResultException, DataAccessException, ParseException {
		try {
			final SelectQuery<Record> query = this.ctx.selectQuery(Tables.EMPLOYEES.join(Tables.SITES_EMPLOYEES)
			                                                       .on(Tables.SITES_EMPLOYEES.SIEM_EMPL_FK.eq(Tables.EMPLOYEES.EMPL_PK)));
			query.addConditions(Tables.SITES_EMPLOYEES.SIEM_UPDT_FK.eq(getUpdateFor(dateStr)));
			if (aurore != null) {
				query.addConditions(Tables.SITES_EMPLOYEES.SIEM_SITE_FK.eq(aurore));
			}

			return query.fetchMaps();
		} catch (final NoDataException e) {
			return Collections.EMPTY_LIST;
		}
	}

	@GET
	@Path("employees/{registrationNumber}")
	public Map<String, Object> lookupEmployee(@PathParam("registrationNumber") final String registrationNumber) {
		return this.ctx.select().from(Tables.EMPLOYEES).where(Tables.EMPLOYEES.EMPL_PK.equal(registrationNumber)).fetchOne().intoMap();
	}

	@GET
	@Path("sites")
	public List<Map<String, Object>> listSites(@QueryParam("department") final Integer department) {
		final SelectQuery<SitesRecord> query = this.ctx.selectQuery(Tables.SITES);
		if (department != null) {
			query.addConditions(Tables.SITES.SITE_DEPT_FK.eq(department));
		}

		return query.fetchMaps();
	}

	@GET
	@Path("sites/{aurore}")
	public Map<String, Object> lookupSite(@PathParam("aurore") final String aurore) {
		return this.ctx.select().from(Tables.SITES).where(Tables.SITES.SITE_PK.equal(aurore)).fetchOne().intoMap();
	}

	private Integer getLatestUpdateWhere(final Condition... conditions) {
		final Record mostAccurateUpdate = this.ctx.select().from(Tables.UPDATES).where(conditions).orderBy(Tables.UPDATES.UPDT_DATE.desc()).fetchAny();
		if (mostAccurateUpdate == null) {
			throw new NoDataException();
		}

		return mostAccurateUpdate.getValue(Tables.UPDATES.UPDT_PK);
	}

	private Integer getUpdateFor(final String dateStr) throws ParseException {
		if (dateStr != null) {
			return getLatestUpdateWhere(Tables.UPDATES.UPDT_DATE.le(this.dateFormat.parseSql(dateStr)));
		}

		return getLatestUpdateWhere();
	}

	@GET
	@Path("trainings")
	public List<Map<String, Object>> listTrainings(@QueryParam("employee") final String registrationNumber, @QueryParam("type") final List<Integer> types) {
		final SelectQuery<Record> query = this.ctx.selectQuery();
		query.addFrom(Tables.TRAININGS);
		if (registrationNumber != null) {
			query.addJoin(
							Tables.TRAININGS_EMPLOYEES,
							Tables.TRAININGS_EMPLOYEES.TREM_TRNG_FK.eq(Tables.TRAININGS.TRNG_PK).and(
																										Tables.TRAININGS_EMPLOYEES.TREM_EMPL_FK
																												.eq(registrationNumber)));
		}

		if (!types.isEmpty()) {
			query.addJoin(Tables.TRAININGTYPES, Tables.TRAININGS.TRNG_TRTY_FK.eq(Tables.TRAININGTYPES.TRTY_PK).and(Tables.TRAININGTYPES.TRTY_PK.in(types)));
		}

		return query.fetchMaps();
	}

	@GET
	@Path("trainings/{id}")
	public Map<String, Object> lookupTraining(@PathParam("id") final Integer id) {
		return this.ctx.select().from(Tables.TRAININGS).where(Tables.TRAININGS.TRNG_PK.equal(id)).fetchOne().intoMap();
	}

	@GET
	@Path("departments")
	public List<Map<String, Object>> listDepartments() {
		return this.ctx.select().from(Tables.DEPARTMENTS).fetchMaps();
	}

	@GET
	@Path("training_types")
	public List<Map<String, Object>> listTrainingTypes() {
		return this.ctx.select().from(Tables.TRAININGTYPES).fetchMaps();
	}

	@GET
	@Path("certificates")
	public List<Map<String, Object>> listCertificates() {
		return this.ctx.select().from(Tables.CERTIFICATES).fetchMaps();
	}
}
