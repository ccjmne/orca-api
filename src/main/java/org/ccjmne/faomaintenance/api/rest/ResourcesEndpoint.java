package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.CERTIFICATES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.DEPARTMENTS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGTYPES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGTYPES_CERTIFICATES;
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
import org.ccjmne.faomaintenance.jooq.classes.tables.records.CertificatesRecord;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.DepartmentsRecord;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.TrainingtypesCertificatesRecord;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.TrainingtypesRecord;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SelectQuery;
import org.jooq.impl.DSL;

@Path("resources")
public class ResourcesEndpoint {

	private static final String SITE_UNASSIGNED = "0";

	private final DSLContext ctx;
	private final SQLDateFormat dateFormat;

	@Inject
	public ResourcesEndpoint(final DSLContext ctx, final SQLDateFormat dateFormat) {
		this.ctx = ctx;
		this.dateFormat = dateFormat;
	}

	@GET
	@Path("employees")
	public Result<Record> listEmployees(
										@QueryParam("site") final String site_pk,
										@QueryParam("date") final String dateStr,
										@QueryParam("training") final String trng_pk) throws ParseException {
		final SelectQuery<Record> query = this.ctx.selectQuery();
		query.addFrom(EMPLOYEES);
		if (site_pk != null) {
			query.addJoin(
							SITES_EMPLOYEES,
							SITES_EMPLOYEES.SIEM_EMPL_FK.eq(EMPLOYEES.EMPL_PK),
							SITES_EMPLOYEES.SIEM_SITE_FK.eq(site_pk),
							SITES_EMPLOYEES.SIEM_UPDT_FK.eq(getUpdateFor(dateStr)));
		} else {
			query.addJoin(
							SITES_EMPLOYEES,
							SITES_EMPLOYEES.SIEM_EMPL_FK.eq(EMPLOYEES.EMPL_PK),
							SITES_EMPLOYEES.SIEM_SITE_FK.notEqual(SITE_UNASSIGNED),
							SITES_EMPLOYEES.SIEM_UPDT_FK.eq(getUpdateFor(dateStr)));
		}

		if (trng_pk != null) {
			query.addJoin(
							TRAININGS_EMPLOYEES,
							TRAININGS_EMPLOYEES.TREM_TRNG_FK.eq(Integer.valueOf(trng_pk)),
							TRAININGS_EMPLOYEES.TREM_EMPL_FK.eq(EMPLOYEES.EMPL_PK));
		}

		return query.fetch();
	}

	@GET
	@Path("employees/{empl_pk}")
	public Record lookupEmployee(@PathParam("empl_pk") final String empl_pk) {
		return this.ctx.selectFrom(EMPLOYEES).where(EMPLOYEES.EMPL_PK.equal(empl_pk)).fetchOne();
	}

	@GET
	@Path("sites")
	public Result<Record> listSites(@QueryParam("department") final Integer dept_pk, @QueryParam("date") final String dateStr)
			throws ParseException {
		final SelectQuery<Record> query = this.ctx.selectQuery();
		query.addSelect(SITES.SITE_PK, SITES.SITE_NAME, SITES.SITE_DEPT_FK);
		query.addFrom(SITES);
		query.addConditions(SITES.SITE_PK.in(DSL.selectDistinct(SITES_EMPLOYEES.SIEM_SITE_FK).from(SITES_EMPLOYEES)
				.where(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(getUpdateFor(dateStr)))));
		query.addConditions(SITES.SITE_PK.notEqual(String.valueOf(0)));

		if (dept_pk != null) {
			query.addConditions(SITES.SITE_DEPT_FK.eq(dept_pk));
		}

		return query.fetch();
	}

	@GET
	@Path("sites/{site_pk}")
	public Record lookupSite(@PathParam("site_pk") final String site_pk) {
		return this.ctx.select().from(SITES).where(SITES.SITE_PK.equal(site_pk)).fetchOne();
	}

	private Integer getUpdateFor(final String dateStr) throws ParseException {
		if (dateStr != null) {
			return this.ctx.selectFrom(UPDATES).where(UPDATES.UPDT_DATE.le(this.dateFormat.parseSql(dateStr))).orderBy(UPDATES.UPDT_DATE.desc())
					.fetchAny(UPDATES.UPDT_PK);
		}

		return this.ctx.selectFrom(UPDATES).orderBy(UPDATES.UPDT_DATE.desc()).fetchAny(UPDATES.UPDT_PK);
	}

	@GET
	@Path("trainings")
	public Result<Record> listTrainings(
										@QueryParam("employee") final String empl_pk,
										@QueryParam("type") final List<Integer> types,
										@QueryParam("date") final String dateStr,
										@QueryParam("from") final String fromStr,
										@QueryParam("to") final String toStr) throws ParseException {
		final SelectQuery<Record> query = this.ctx.selectQuery();
		query.addFrom(TRAININGS);
		if (empl_pk != null) {
			query.addJoin(
							TRAININGS_EMPLOYEES,
							TRAININGS_EMPLOYEES.TREM_TRNG_FK.eq(TRAININGS.TRNG_PK).and(TRAININGS_EMPLOYEES.TREM_EMPL_FK.eq(empl_pk)));
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
	@Path("trainings/{trng_pk}")
	public Record lookupTraining(@PathParam("trng_pk") final Integer trng_pk) {
		return this.ctx.selectFrom(TRAININGS).where(TRAININGS.TRNG_PK.equal(trng_pk)).fetchOne();
	}

	@POST
	@Path("trainings")
	public Integer addTraining(final Map<String, Object> training) throws ParseException {
		return insertTraining(new Integer(this.ctx.nextval(Sequences.TRAININGS_TRNG_PK_SEQ).intValue()), training);
	}

	@SuppressWarnings("unchecked")
	private Integer insertTraining(final Integer trng_pk, final Map<String, Object> map) throws ParseException {
		this.ctx
				.insertInto(TRAININGS, TRAININGS.TRNG_PK, TRAININGS.TRNG_TRTY_FK, TRAININGS.TRNG_DATE, TRAININGS.TRNG_OUTCOME)
				.values(
						trng_pk,
						(Integer) map.get("trng_trty_fk"),
						this.dateFormat.parseSql(map.get("trng_date").toString()), map.get("trng_outcome").toString()).execute();
		((Map<String, Map<String, String>>) map.getOrDefault("trainees", Collections.EMPTY_MAP)).forEach((trainee, info) -> this.ctx
				.insertInto(
							TRAININGS_EMPLOYEES,
							TRAININGS_EMPLOYEES.TREM_TRNG_FK,
							TRAININGS_EMPLOYEES.TREM_EMPL_FK,
							TRAININGS_EMPLOYEES.TREM_VALID,
							TRAININGS_EMPLOYEES.TREM_COMMENT)
				.values(trng_pk, trainee, Boolean.valueOf(String.valueOf(info.get("trem_valid"))), info.get("trem_comment")).execute());
		return trng_pk;
	}

	@PUT
	@Path("trainings/{trng_pk}")
	public boolean updateTraining(@PathParam("trng_pk") final Integer trng_pk, final Map<String, Object> training) throws ParseException {
		final boolean exists = deleteTraining(trng_pk);
		insertTraining(trng_pk, training);
		return exists;
	}

	@DELETE
	@Path("trainings/{trng_pk}")
	public boolean deleteTraining(@PathParam("trng_pk") final Integer trng_pk) {
		final boolean exists = this.ctx.selectFrom(TRAININGS).where(TRAININGS.TRNG_PK.equal(trng_pk)).fetch().isNotEmpty();
		if (exists) {
			this.ctx.delete(TRAININGS).where(TRAININGS.TRNG_PK.equal(trng_pk)).execute();
		}

		return exists;
	}

	@GET
	@Path("departments")
	public Result<DepartmentsRecord> listDepartments() {
		return this.ctx.selectFrom(DEPARTMENTS).fetch();
	}

	@GET
	@Path("trainingtypes")
	public Result<TrainingtypesRecord> listTrainingTypes() {
		return this.ctx.selectFrom(TRAININGTYPES).fetch();
	}

	@GET
	@Path("trainingtypes_certificates")
	public Result<TrainingtypesCertificatesRecord> listTrainingTypesCertificates() {
		return this.ctx.selectFrom(TRAININGTYPES_CERTIFICATES).fetch();
	}

	@GET
	@Path("certificates")
	public Result<CertificatesRecord> listCertificates() {
		return this.ctx.selectFrom(CERTIFICATES).fetch();
	}
}
