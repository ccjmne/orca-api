package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.CERTIFICATES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.DEPARTMENTS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES_CERTIFICATES_OPTOUT;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS_TRAINERS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGTYPES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGTYPES_CERTIFICATES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.UPDATES;

import java.sql.Date;
import java.text.ParseException;
import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.ccjmne.faomaintenance.api.rest.resources.TrainingsStatistics;
import org.ccjmne.faomaintenance.api.utils.SQLDateFormat;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.CertificatesRecord;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.DepartmentsRecord;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.EmployeesCertificatesOptoutRecord;
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
		try (final SelectQuery<Record> query = this.ctx.selectQuery()) {
			query.addSelect(
							EMPLOYEES.EMPL_PK,
							EMPLOYEES.EMPL_FIRSTNAME,
							EMPLOYEES.EMPL_SURNAME,
							EMPLOYEES.EMPL_DOB,
							EMPLOYEES.EMPL_PERMANENT,
							EMPLOYEES.EMPL_GENDER,
							EMPLOYEES.EMPL_NOTES,
							EMPLOYEES.EMPL_ADDR);
			query.addSelect(SITES_EMPLOYEES.fields());
			query.addFrom(EMPLOYEES);
			if (site_pk != null) {
				query.addJoin(
								SITES_EMPLOYEES,
								SITES_EMPLOYEES.SIEM_EMPL_FK.eq(EMPLOYEES.EMPL_PK),
								SITES_EMPLOYEES.SIEM_SITE_FK.eq(site_pk),
								SITES_EMPLOYEES.SIEM_UPDT_FK.eq(getUpdatePkFor(dateStr)));
			} else {
				query.addJoin(
								SITES_EMPLOYEES,
								SITES_EMPLOYEES.SIEM_EMPL_FK.eq(EMPLOYEES.EMPL_PK),
								SITES_EMPLOYEES.SIEM_SITE_FK.ne(SITE_UNASSIGNED),
								SITES_EMPLOYEES.SIEM_UPDT_FK.eq(getUpdatePkFor(dateStr)));
			}

			if (trng_pk != null) {
				query.addSelect(TRAININGS_EMPLOYEES.fields());
				query.addJoin(
								TRAININGS_EMPLOYEES,
								TRAININGS_EMPLOYEES.TREM_TRNG_FK.eq(Integer.valueOf(trng_pk)),
								TRAININGS_EMPLOYEES.TREM_EMPL_FK.eq(EMPLOYEES.EMPL_PK));
			}

			return query.fetch();
		}
	}

	@GET
	@Path("employees/{empl_pk}")
	public Record lookupEmployee(@PathParam("empl_pk") final String empl_pk) {
		return this.ctx
				.select(
						EMPLOYEES.EMPL_PK,
						EMPLOYEES.EMPL_FIRSTNAME,
						EMPLOYEES.EMPL_SURNAME,
						EMPLOYEES.EMPL_DOB,
						EMPLOYEES.EMPL_PERMANENT,
						EMPLOYEES.EMPL_GENDER,
						EMPLOYEES.EMPL_NOTES,
						EMPLOYEES.EMPL_ADDR)
				.from(EMPLOYEES).where(EMPLOYEES.EMPL_PK.equal(empl_pk)).fetchOne();

	}

	@GET
	@Path("employees/{empl_pk}/voiding")
	public Result<EmployeesCertificatesOptoutRecord> getEmployeeCertificatesVoiding(@PathParam("empl_pk") final String empl_pk) {
		return this.ctx.selectFrom(EMPLOYEES_CERTIFICATES_OPTOUT).where(EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_EMPL_FK.eq(empl_pk))
				.orderBy(EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_CERT_FK).fetch();
	}

	@GET
	@Path("sites")
	public Result<Record> listSites(
									@QueryParam("department") final Integer dept_pk,
									@QueryParam("employee") final String empl_pk,
									@QueryParam("date") final String dateStr,
									@QueryParam("unlisted") final boolean unlisted)
											throws ParseException {
		try (final SelectQuery<Record> query = this.ctx.selectQuery()) {
			query.addSelect(SITES.SITE_PK, SITES.SITE_NAME, SITES.SITE_DEPT_FK, SITES.SITE_NOTES);
			query.addFrom(SITES);
			if (!unlisted) {
				query.addConditions(SITES.SITE_PK.in(DSL.selectDistinct(SITES_EMPLOYEES.SIEM_SITE_FK).from(SITES_EMPLOYEES)
						.where(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(getUpdatePkFor(dateStr)))));
			}
			query.addConditions(SITES.SITE_PK.notEqual(String.valueOf(0)));

			if (dept_pk != null) {
				query.addConditions(SITES.SITE_DEPT_FK.eq(dept_pk));
			}

			if (empl_pk != null) {
				query.addConditions(SITES.SITE_PK.eq(DSL.select(SITES_EMPLOYEES.SIEM_SITE_FK).from(SITES_EMPLOYEES)
						.where(SITES_EMPLOYEES.SIEM_EMPL_FK.eq(empl_pk).and(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(getUpdatePkFor(dateStr))))));
			}

			return query.fetch();
		}
	}

	@GET
	@Path("sites/{site_pk}")
	public Record lookupSite(@PathParam("site_pk") final String site_pk) {
		return this.ctx.select().from(SITES).where(SITES.SITE_PK.equal(site_pk)).fetchOne();
	}

	private Integer getUpdatePkFor(final String dateStr) throws ParseException {
		return getUpdateFor(dateStr).getValue(UPDATES.UPDT_PK);
	}

	@GET
	@Path("update")
	public Record getUpdateFor(@QueryParam("date") final String dateStr) throws ParseException {
		if (dateStr != null) {
			return this.ctx.selectFrom(UPDATES).where(UPDATES.UPDT_DATE.le(this.dateFormat.parseSql(dateStr))).orderBy(UPDATES.UPDT_DATE.desc())
					.fetchAny();
		}

		return this.ctx.selectFrom(UPDATES).orderBy(UPDATES.UPDT_DATE.desc()).fetchAny();

	}

	@GET
	@Path("trainings")
	public Result<Record> listTrainings(
										@QueryParam("employee") final String empl_pk,
										@QueryParam("type") final List<Integer> types,
										@QueryParam("date") final String dateStr,
										@QueryParam("from") final String fromStr,
										@QueryParam("to") final String toStr) throws ParseException {
		try (final SelectQuery<Record> query = this.ctx.selectQuery()) {
			query.addSelect(TRAININGS.fields());
			query.addFrom(TRAININGS);
			query.addGroupBy(TRAININGS.fields());

			query.addSelect(DSL.select(TrainingsStatistics.AGENTS_REGISTERED).from(TRAININGS_EMPLOYEES)
					.where(TRAININGS_EMPLOYEES.TREM_TRNG_FK.eq(TRAININGS.TRNG_PK))
					.asField("registered"));
			query.addSelect(DSL.select(TrainingsStatistics.AGENTS_VALIDATED).from(TRAININGS_EMPLOYEES)
					.where(TRAININGS_EMPLOYEES.TREM_TRNG_FK.eq(TRAININGS.TRNG_PK))
					.asField("validated"));
			query.addSelect(DSL.select(DSL.count(TRAININGS_EMPLOYEES.TREM_OUTCOME)
					.filterWhere(TRAININGS_EMPLOYEES.TREM_OUTCOME.eq("FLUNKED"))).from(TRAININGS_EMPLOYEES)
					.where(TRAININGS_EMPLOYEES.TREM_TRNG_FK.eq(TRAININGS.TRNG_PK))
					.asField("flunked"));
			query.addSelect(DSL.select(DSL.arrayAgg(TRAININGS_TRAINERS.TRTR_EMPL_FK)).from(TRAININGS_TRAINERS)
					.where(TRAININGS_TRAINERS.TRTR_TRNG_FK.eq(TRAININGS.TRNG_PK))
					.asField("trainers"));

			if (empl_pk != null) {
				query.addSelect(TRAININGS_EMPLOYEES.TREM_OUTCOME, TRAININGS_EMPLOYEES.TREM_COMMENT);
				query.addGroupBy(TRAININGS_EMPLOYEES.TREM_OUTCOME, TRAININGS_EMPLOYEES.TREM_COMMENT);
				query.addJoin(TRAININGS_EMPLOYEES, TRAININGS_EMPLOYEES.TREM_TRNG_FK.eq(TRAININGS.TRNG_PK).and(TRAININGS_EMPLOYEES.TREM_EMPL_FK.eq(empl_pk)));
			}

			if (!types.isEmpty()) {
				query.addJoin(TRAININGTYPES, TRAININGS.TRNG_TRTY_FK.eq(TRAININGTYPES.TRTY_PK).and(TRAININGTYPES.TRTY_PK.in(types)));
			}

			if (dateStr != null) {
				final Date date = this.dateFormat.parseSql(dateStr);
				query.addConditions(TRAININGS.TRNG_START.isNotNull()
						.and(TRAININGS.TRNG_START.le(date).and(TRAININGS.TRNG_DATE.ge(date)))
						.or(TRAININGS.TRNG_DATE.eq(date)));
			}

			if (fromStr != null) {
				final Date from = this.dateFormat.parseSql(fromStr);
				query.addConditions(TRAININGS.TRNG_DATE.ge(from).or(TRAININGS.TRNG_START.isNotNull().and(TRAININGS.TRNG_START.ge(from))));
			}

			if (toStr != null) {
				final Date to = this.dateFormat.parseSql(toStr);
				query.addConditions(TRAININGS.TRNG_DATE.le(to).or(TRAININGS.TRNG_START.isNotNull().and(TRAININGS.TRNG_START.le(to))));
			}

			query.addOrderBy(TRAININGS.TRNG_DATE);
			return query.fetch();
		}
	}

	@GET
	@Path("trainings/{trng_pk}")
	public Record lookupTraining(@PathParam("trng_pk") final Integer trng_pk) {
		try (final SelectQuery<Record> query = this.ctx.selectQuery()) {
			query.addSelect(TRAININGS.fields());
			query.addSelect(DSL.select(TrainingsStatistics.AGENTS_REGISTERED).from(TRAININGS_EMPLOYEES)
					.where(TRAININGS_EMPLOYEES.TREM_TRNG_FK.eq(TRAININGS.TRNG_PK))
					.asField("registered"));
			query.addSelect(DSL.select(TrainingsStatistics.AGENTS_VALIDATED).from(TRAININGS_EMPLOYEES)
					.where(TRAININGS_EMPLOYEES.TREM_TRNG_FK.eq(TRAININGS.TRNG_PK))
					.asField("validated"));
			query.addSelect(DSL.select(DSL.count(TRAININGS_EMPLOYEES.TREM_OUTCOME)
					.filterWhere(TRAININGS_EMPLOYEES.TREM_OUTCOME.eq("FLUNKED"))).from(TRAININGS_EMPLOYEES)
					.where(TRAININGS_EMPLOYEES.TREM_TRNG_FK.eq(TRAININGS.TRNG_PK))
					.asField("flunked"));
			query.addSelect(DSL.select(DSL.arrayAgg(TRAININGS_TRAINERS.TRTR_EMPL_FK)).from(TRAININGS_TRAINERS)
					.where(TRAININGS_TRAINERS.TRTR_TRNG_FK.eq(TRAININGS.TRNG_PK))
					.asField("trainers"));
			query.addFrom(TRAININGS);
			query.addConditions(TRAININGS.TRNG_PK.eq(trng_pk));
			query.addGroupBy(TRAININGS.fields());
			return query.fetchOne();
		}
	}

	@GET
	@Path("departments")
	public Result<DepartmentsRecord> listDepartments() {
		return this.ctx.selectFrom(DEPARTMENTS).fetch();
	}

	@GET
	@Path("trainingtypes")
	public Result<TrainingtypesRecord> listTrainingTypes() {
		return this.ctx.selectFrom(TRAININGTYPES).orderBy(TRAININGTYPES.TRTY_ORDER).fetch();
	}

	@GET
	@Path("trainingtypes_certificates")
	public Result<TrainingtypesCertificatesRecord> listTrainingTypesCertificates() {
		return this.ctx.selectFrom(TRAININGTYPES_CERTIFICATES).fetch();
	}

	@GET
	@Path("certificates")
	public Result<CertificatesRecord> listCertificates() {
		return this.ctx.selectFrom(CERTIFICATES).orderBy(CERTIFICATES.CERT_ORDER).fetch();
	}
}
