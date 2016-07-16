package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.DEPARTMENTS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGTYPES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.UPDATES;

import java.sql.Date;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.ccjmne.faomaintenance.api.modules.ResourcesUnrestricted;
import org.ccjmne.faomaintenance.api.modules.Restrictions;
import org.ccjmne.faomaintenance.api.utils.Constants;
import org.ccjmne.faomaintenance.api.utils.SafeDateFormat;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.CertificatesRecord;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.EmployeesCertificatesOptoutRecord;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.EmployeesRecord;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.TrainingtypesCertificatesRecord;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.TrainingtypesRecord;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.UpdatesRecord;
import org.jooq.DSLContext;
import org.jooq.JoinType;
import org.jooq.Record;
import org.jooq.Record4;
import org.jooq.Result;
import org.jooq.Select;
import org.jooq.SelectQuery;
import org.jooq.impl.DSL;

@Path("resources")
public class ResourcesEndpoint {

	private final DSLContext ctx;
	private final ResourcesUnrestricted unrestrictedResources;
	private final Restrictions restrictions;

	@Inject
	public ResourcesEndpoint(final DSLContext ctx, final ResourcesUnrestricted unrestrictedResources, final Restrictions restrictions) {
		this.ctx = ctx;
		this.unrestrictedResources = unrestrictedResources;
		this.restrictions = restrictions;
	}

	@GET
	@Path("updates")
	// TODO: move to UpdateEndpoint?
	public Result<UpdatesRecord> listUpdates() {
		return this.ctx.selectFrom(UPDATES).orderBy(UPDATES.UPDT_DATE.desc()).fetch();
	}

	@GET
	@Path("employees")
	public Result<Record> listEmployees(
										@QueryParam("site") final String site_pk,
										@QueryParam("date") final String dateStr,
										@QueryParam("training") final String trng_pk) throws ParseException {
		if ((site_pk != null) && !this.restrictions.canAccessAllSites() && !this.restrictions.getAccessibleSites().contains(site_pk)) {
			throw new ForbiddenException();
		}

		try (final SelectQuery<Record> query = this.ctx.selectQuery()) {
			query.addSelect(EMPLOYEES.fields());
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
								SITES_EMPLOYEES.SIEM_UPDT_FK.eq(getUpdatePkFor(dateStr)));

				if (trng_pk == null) {
					query.addConditions(SITES_EMPLOYEES.SIEM_SITE_FK.ne(Constants.UNASSIGNED_SITE));
				}

				if (!this.restrictions.canAccessAllSites()) {
					query.addConditions(SITES_EMPLOYEES.SIEM_SITE_FK.in(this.restrictions.getAccessibleSites()));
				}
			}

			if (trng_pk != null) {
				if (!this.restrictions.canAccessTrainings()) {
					throw new ForbiddenException();
				}

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
	public EmployeesRecord lookupEmployee(@PathParam("empl_pk") final String empl_pk) {
		if (!this.restrictions.canAccessEmployee(empl_pk)) {
			throw new ForbiddenException();
		}

		return this.ctx.selectFrom(EMPLOYEES).where(EMPLOYEES.EMPL_PK.equal(empl_pk)).fetchOne();
	}

	@GET
	@Path("employees/{empl_pk}/voiding")
	public Result<EmployeesCertificatesOptoutRecord> getEmployeeVoiding(@PathParam("empl_pk") final String empl_pk) {
		if (!this.restrictions.canAccessEmployee(empl_pk)) {
			throw new ForbiddenException();
		}

		return this.unrestrictedResources.listCertificatesVoiding(empl_pk);
	}

	@GET
	@Path("sites")
	public Result<Record> listSites(
									@QueryParam("department") final Integer dept_pk,
									@QueryParam("employee") final String empl_pk,
									@QueryParam("date") final String dateStr,
									@QueryParam("unlisted") final boolean unlisted) throws ParseException {
		if ((empl_pk != null) && !this.restrictions.canAccessEmployee(empl_pk)) {
			throw new ForbiddenException();
		}

		if ((dept_pk != null) && !this.restrictions.canAccessDepartment(dept_pk)) {
			throw new ForbiddenException();
		}

		final Integer update = getUpdatePkFor(dateStr);
		try (
				final SelectQuery<Record> query = this.ctx.selectQuery();
				final Select<? extends Record> employees = DSL
						.select(SITES_EMPLOYEES.SIEM_SITE_FK).from(SITES_EMPLOYEES)
						.where(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(update))) {
			query.addSelect(SITES.SITE_PK, SITES.SITE_NAME, SITES.SITE_DEPT_FK, SITES.SITE_NOTES, DSL.count(employees.field(SITES_EMPLOYEES.SIEM_SITE_FK)));
			query.addGroupBy(SITES.SITE_PK, SITES.SITE_NAME, SITES.SITE_DEPT_FK, SITES.SITE_NOTES);
			query.addFrom(SITES);
			query.addJoin(
							employees,
							unlisted ? JoinType.LEFT_OUTER_JOIN : JoinType.JOIN,
							employees.field(SITES_EMPLOYEES.SIEM_SITE_FK).eq(SITES.SITE_PK));
			query.addConditions(SITES.SITE_PK.ne(Constants.UNASSIGNED_SITE));

			if (dept_pk != null) {
				query.addConditions(SITES.SITE_DEPT_FK.eq(dept_pk));
			}

			if (empl_pk != null) {
				query.addConditions(SITES.SITE_PK.eq(DSL.select(SITES_EMPLOYEES.SIEM_SITE_FK).from(SITES_EMPLOYEES)
						.where(SITES_EMPLOYEES.SIEM_EMPL_FK.eq(empl_pk).and(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(update)))));
			}

			if (!this.restrictions.canAccessAllSites()) {
				query.addConditions(SITES.SITE_PK.in(this.restrictions.getAccessibleSites()));
			}

			return query.fetch();
		}
	}

	@GET
	@Path("sites/{site_pk}")
	public Record lookupSite(@PathParam("site_pk") final String site_pk) {
		if (!this.restrictions.canAccessAllSites() && !this.restrictions.getAccessibleSites().contains(site_pk)) {
			throw new ForbiddenException();
		}

		return this.ctx.selectFrom(SITES).where(SITES.SITE_PK.eq(site_pk)).fetchOne();
	}

	@GET
	@Path("sites/{site_pk}/history")
	public Map<Date, Result<Record4<String, Boolean, String, Date>>> getSiteEmployeesHistory(@PathParam("site_pk") final String site_pk) {
		if (!this.restrictions.canAccessAllSites() && !this.restrictions.getAccessibleSites().contains(site_pk)) {
			throw new ForbiddenException();
		}

		return this.ctx.select(EMPLOYEES.EMPL_PK, EMPLOYEES.EMPL_PERMANENT, SITES_EMPLOYEES.SIEM_SITE_FK, UPDATES.UPDT_DATE).from(SITES_EMPLOYEES)
				.join(EMPLOYEES).on(EMPLOYEES.EMPL_PK.eq(SITES_EMPLOYEES.SIEM_EMPL_FK))
				.join(UPDATES).on(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(UPDATES.UPDT_PK))
				.where(SITES_EMPLOYEES.SIEM_SITE_FK.eq(site_pk))
				.fetchGroups(UPDATES.UPDT_DATE);
	}

	@GET
	@Path("update")
	// TODO: move to UpdateEndpoint?
	public Record getUpdateFor(@QueryParam("date") final String dateStr) throws ParseException {
		if (dateStr != null) {
			return this.ctx.selectFrom(UPDATES)
					.where(UPDATES.UPDT_DATE.eq(DSL
							.select(DSL.max(UPDATES.UPDT_DATE)).from(UPDATES)
							.where(UPDATES.UPDT_DATE.le(SafeDateFormat.parseAsSql(dateStr)))))
					.fetchAny();
		}

		return this.ctx.selectFrom(UPDATES).where(UPDATES.UPDT_DATE.eq(DSL.select(DSL.max(UPDATES.UPDT_DATE)).from(UPDATES))).fetchAny();
	}

	@GET
	@Path("trainings")
	public Result<Record> listTrainings(
										@QueryParam("employee") final String empl_pk,
										@QueryParam("type") final List<Integer> types,
										@QueryParam("date") final String dateStr,
										@QueryParam("from") final String fromStr,
										@QueryParam("to") final String toStr,
										@QueryParam("completed") final Boolean completedOnly) throws ParseException {
		if (!this.restrictions.canAccessTrainings()) {
			throw new ForbiddenException();
		}

		final Date date = dateStr == null ? null : SafeDateFormat.parseAsSql(dateStr);
		final Date from = fromStr == null ? null : SafeDateFormat.parseAsSql(fromStr);
		final Date to = toStr == null ? null : SafeDateFormat.parseAsSql(toStr);

		try (final SelectQuery<Record> query = this.ctx.selectQuery()) {
			query.addSelect(TRAININGS.fields());
			query.addFrom(TRAININGS);
			query.addGroupBy(TRAININGS.fields());
			query.addJoin(TRAININGS_EMPLOYEES, JoinType.LEFT_OUTER_JOIN, TRAININGS_EMPLOYEES.TREM_TRNG_FK.eq(TRAININGS.TRNG_PK));

			query.addSelect(Constants.TRAINING_REGISTERED);
			query.addSelect(Constants.TRAINING_VALIDATED);
			query.addSelect(Constants.TRAINING_FLUNKED);
			query.addSelect(Constants.TRAINING_EXPIRY);
			query.addSelect(Constants.TRAINING_TRAINERS);

			if (empl_pk != null) {
				query.addSelect(TRAININGS_EMPLOYEES.TREM_OUTCOME, TRAININGS_EMPLOYEES.TREM_COMMENT);
				query.addGroupBy(TRAININGS_EMPLOYEES.TREM_OUTCOME, TRAININGS_EMPLOYEES.TREM_COMMENT);
				query.addConditions(TRAININGS_EMPLOYEES.TREM_PK
						.in(DSL.select(TRAININGS_EMPLOYEES.TREM_PK).from(TRAININGS_EMPLOYEES).where(TRAININGS_EMPLOYEES.TREM_EMPL_FK.eq(empl_pk))));
			}

			if (!types.isEmpty()) {
				query.addJoin(TRAININGTYPES, TRAININGS.TRNG_TRTY_FK.eq(TRAININGTYPES.TRTY_PK).and(TRAININGTYPES.TRTY_PK.in(types)));
			}

			if (date != null) {
				query.addConditions(TRAININGS.TRNG_START.isNotNull()
						.and(TRAININGS.TRNG_START.le(date).and(TRAININGS.TRNG_DATE.ge(date)))
						.or(TRAININGS.TRNG_DATE.eq(date)));
			}

			if (from != null) {
				query.addConditions(TRAININGS.TRNG_DATE.ge(from).or(TRAININGS.TRNG_START.isNotNull().and(TRAININGS.TRNG_START.ge(from))));
			}

			if (to != null) {
				query.addConditions(TRAININGS.TRNG_DATE.le(to).or(TRAININGS.TRNG_START.isNotNull().and(TRAININGS.TRNG_START.le(to))));
			}

			if ((completedOnly != null) && completedOnly.booleanValue()) {
				query.addConditions(TRAININGS.TRNG_OUTCOME.eq(Constants.TRNG_OUTCOME_COMPLETED));
			}

			query.addOrderBy(TRAININGS.TRNG_DATE);
			return query.fetch();
		}
	}

	@GET
	@Path("trainings/{trng_pk}")
	public Record lookupTraining(@PathParam("trng_pk") final Integer trng_pk) {
		if (!this.restrictions.canAccessTrainings()) {
			throw new ForbiddenException();
		}

		try (final SelectQuery<Record> query = this.ctx.selectQuery()) {
			query.addSelect(TRAININGS.fields());
			query.addFrom(TRAININGS);
			query.addJoin(TRAININGS_EMPLOYEES, JoinType.LEFT_OUTER_JOIN, TRAININGS_EMPLOYEES.TREM_TRNG_FK.eq(TRAININGS.TRNG_PK));
			query.addSelect(Constants.TRAINING_REGISTERED);
			query.addSelect(Constants.TRAINING_VALIDATED);
			query.addSelect(Constants.TRAINING_FLUNKED);
			query.addSelect(Constants.TRAINING_EXPIRY);
			query.addSelect(Constants.TRAINING_TRAINERS);
			query.addConditions(TRAININGS.TRNG_PK.eq(trng_pk));
			query.addGroupBy(TRAININGS.fields());
			return query.fetchOne();
		}
	}

	@GET
	@Path("departments")
	public Result<? extends Record> listDepartments(@QueryParam("unlisted") final boolean unlisted) {
		if (!this.restrictions.canAccessAllSites() && (this.restrictions.getAccessibleDepartment() == null)) {
			throw new ForbiddenException();
		}

		try (final SelectQuery<Record> query = this.ctx.selectQuery()) {
			query.addSelect(DEPARTMENTS.fields());
			query.addSelect(DSL.count(SITES.SITE_PK));
			query.addFrom(DEPARTMENTS);
			query.addGroupBy(DEPARTMENTS.fields());
			query.addJoin(SITES, unlisted ? JoinType.LEFT_OUTER_JOIN : JoinType.JOIN, SITES.SITE_DEPT_FK.eq(DEPARTMENTS.DEPT_PK));
			if (!this.restrictions.canAccessAllSites()) {
				query.addConditions(DEPARTMENTS.DEPT_PK.eq(this.restrictions.getAccessibleDepartment()));
			}
			query.addConditions(DEPARTMENTS.DEPT_PK.ne(DSL.val(Constants.UNASSIGNED_DEPT)));
			return query.fetch();
		}
	}

	@GET
	@Path("trainingtypes")
	public Result<TrainingtypesRecord> listTrainingTypes() {
		return this.unrestrictedResources.listTrainingTypes();
	}

	@GET
	@Path("trainingtypes_certificates")
	public Result<TrainingtypesCertificatesRecord> listTrainingTypesCertificates() {
		return this.unrestrictedResources.listTrainingTypesCertificates();
	}

	@GET
	@Path("certificates")
	public Result<CertificatesRecord> listCertificates() {
		return this.unrestrictedResources.listCertificates();
	}

	private Integer getUpdatePkFor(final String dateStr) throws ParseException {
		return getUpdateFor(dateStr).getValue(UPDATES.UPDT_PK);
	}
}
