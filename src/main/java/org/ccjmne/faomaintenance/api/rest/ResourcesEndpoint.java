package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.CERTIFICATES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.DEPARTMENTS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGTYPES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.UPDATES;

import java.text.ParseException;
import java.util.Collections;
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

import org.ccjmne.faomaintenance.api.utils.SQLDateFormat;
import org.ccjmne.faomaintenance.jooq.classes.Sequences;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.SitesRecord;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SelectQuery;

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
	public Result<Record> listEmployees(@QueryParam("site") final String aurore, @QueryParam("date") final String dateStr) throws ParseException {
		final SelectQuery<Record> query = this.ctx.selectQuery(EMPLOYEES.join(SITES_EMPLOYEES).on(SITES_EMPLOYEES.SIEM_EMPL_FK.eq(EMPLOYEES.EMPL_PK)));
		query.addConditions(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(getUpdateFor(dateStr)));
		if (aurore != null) {
			query.addConditions(SITES_EMPLOYEES.SIEM_SITE_FK.eq(aurore));
		}

		return query.fetch();
	}

	@GET
	@Path("employees/{registrationNumber}")
	public Record lookupEmployee(@PathParam("registrationNumber") final String registrationNumber) {
		return this.ctx.select().from(EMPLOYEES).where(EMPLOYEES.EMPL_PK.equal(registrationNumber)).fetchOne();
	}

	@GET
	@Path("sites")
	public Result<SitesRecord> listSites(@QueryParam("department") final Integer department) {
		final SelectQuery<SitesRecord> query = this.ctx.selectQuery(SITES);
		if (department != null) {
			query.addConditions(SITES.SITE_DEPT_FK.eq(department));
		}

		return query.fetch();
	}

	@GET
	@Path("sites/{aurore}")
	public Record lookupSite(@PathParam("aurore") final String aurore) {
		return this.ctx.select().from(SITES).where(SITES.SITE_PK.equal(aurore)).fetchOne();
	}

	private Integer getLatestUpdateWhere(final Condition... conditions) {
		return this.ctx.select().from(UPDATES).where(conditions).orderBy(UPDATES.UPDT_DATE.desc()).fetchAny(UPDATES.UPDT_PK);
	}

	private Integer getUpdateFor(final String dateStr) throws ParseException {
		if (dateStr != null) {
			return getLatestUpdateWhere(UPDATES.UPDT_DATE.le(this.dateFormat.parseSql(dateStr)));
		}

		return getLatestUpdateWhere();
	}

	@GET
	@Path("trainings")
	public Result<Record> listTrainings(
										@QueryParam("employee") final String registrationNumber,
										@QueryParam("type") final List<Integer> types,
										@QueryParam("date") final String dateStr,
										@QueryParam("from") final String fromStr,
										@QueryParam("to") final String toStr) throws ParseException {
		final SelectQuery<Record> query = this.ctx.selectQuery();
		query.addFrom(TRAININGS);
		if (registrationNumber != null) {
			query.addJoin(
							TRAININGS_EMPLOYEES,
							TRAININGS_EMPLOYEES.TREM_TRNG_FK.eq(TRAININGS.TRNG_PK).and(TRAININGS_EMPLOYEES.TREM_EMPL_FK.eq(registrationNumber)));
		}

		if (!types.isEmpty()) {
			query.addJoin(TRAININGTYPES, TRAININGS.TRNG_TRTY_FK.eq(TRAININGTYPES.TRTY_PK).and(TRAININGTYPES.TRTY_PK.in(types)));
		}

		if (dateStr != null) {
			query.addConditions(TRAININGS.TRNG_DATE.eq(this.dateFormat.parseSql(dateStr)));
		}

		if (fromStr != null) {
			query.addConditions(TRAININGS.TRNG_DATE.ge(this.dateFormat.parseSql(fromStr)));
		}

		if (toStr != null) {
			query.addConditions(TRAININGS.TRNG_DATE.le(this.dateFormat.parseSql(toStr)));
		}

		query.addOrderBy(TRAININGS.TRNG_DATE);
		return query.fetch();
	}

	@GET
	@Path("trainings/{id}")
	public Record lookupTraining(@PathParam("id") final Integer id) {
		return this.ctx.select().from(TRAININGS).where(TRAININGS.TRNG_PK.equal(id)).fetchOne();
	}

	@POST
	@Path("trainings")
	public Integer addTraining(final Map<String, Object> training) throws ParseException {
		return insertTraining(new Integer(this.ctx.nextval(Sequences.TRAININGS_TRNG_PK_SEQ).intValue()), training);
	}

	@SuppressWarnings("unchecked")
	private Integer insertTraining(final Integer trainingId, final Map<String, Object> map) throws ParseException {
		this.ctx
				.insertInto(TRAININGS, TRAININGS.TRNG_PK, TRAININGS.TRNG_TRTY_FK, TRAININGS.TRNG_DATE, TRAININGS.TRNG_OUTCOME)
				.values(
						trainingId,
						(Integer) map.get("trng_trty_fk"),
						this.dateFormat.parseSql(map.get("trng_date").toString()), map.get("trng_outcome").toString()).execute();
		((Map<String, Map<String, String>>) map.getOrDefault("trainees", Collections.EMPTY_MAP)).forEach((trainee, info) -> this.ctx
				.insertInto(
							TRAININGS_EMPLOYEES,
							TRAININGS_EMPLOYEES.TREM_TRNG_FK,
							TRAININGS_EMPLOYEES.TREM_EMPL_FK,
							TRAININGS_EMPLOYEES.TREM_VALID,
							TRAININGS_EMPLOYEES.TREM_COMMENT)
				.values(trainingId, trainee, Boolean.valueOf(String.valueOf(info.get("trem_valid"))), info.get("trem_comment")).execute());
		return trainingId;
	}

	@PUT
	@Path("trainings/{id}")
	public boolean updateTraining(@PathParam("id") final Integer id, final Map<String, Object> training) throws ParseException {
		final boolean exists = deleteTraining(id);
		insertTraining(id, training);
		return exists;
	}

	@DELETE
	@Path("trainings/{id}")
	public boolean deleteTraining(final Integer trainingId) {
		final boolean exists = this.ctx.select().from(TRAININGS).where(TRAININGS.TRNG_PK.equal(trainingId)).fetch().isNotEmpty();
		if (exists) {
			this.ctx.delete(TRAININGS).where(TRAININGS.TRNG_PK.equal(trainingId)).execute();
		}

		return exists;
	}

	@GET
	@Path("departments")
	public Result<Record> listDepartments() {
		return this.ctx.select().from(DEPARTMENTS).fetch();
	}

	@GET
	@Path("training_types")
	public Result<Record> listTrainingTypes() {
		return this.ctx.select().from(TRAININGTYPES).fetch();
	}

	@GET
	@Path("certificates")
	public Result<Record> listCertificates() {
		return this.ctx.select().from(CERTIFICATES).fetch();
	}
}
