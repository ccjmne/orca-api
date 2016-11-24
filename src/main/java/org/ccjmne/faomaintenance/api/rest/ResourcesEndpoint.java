package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.DEPARTMENTS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES_CERTIFICATES_OPTOUT;
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
import org.ccjmne.faomaintenance.jooq.classes.tables.records.EmployeesCertificatesOptoutRecord;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.UpdatesRecord;
import org.jooq.DSLContext;
import org.jooq.JoinType;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Select;
import org.jooq.SelectQuery;
import org.jooq.impl.DSL;

@Path("resources")
public class ResourcesEndpoint {

	private final DSLContext ctx;
	private final Restrictions restrictions;

	@Inject
	public ResourcesEndpoint(final DSLContext ctx, final ResourcesUnrestricted unrestrictedResources, final Restrictions restrictions) {
		this.ctx = ctx;
		this.restrictions = restrictions;
	}

	public SelectQuery<Record> selectEmployees(final String empl_pk, final String site_pk, final Integer dept_pk, final Integer trng_pk, final String dateStr) {
		final SelectQuery<Record> query = DSL.select().getQuery();
		query.addFrom(EMPLOYEES);
		query.addJoin(
						SITES_EMPLOYEES,
						SITES_EMPLOYEES.SIEM_EMPL_FK.eq(EMPLOYEES.EMPL_PK),
						SITES_EMPLOYEES.SIEM_UPDT_FK.eq(Constants.selectUpdate(dateStr)),
						SITES_EMPLOYEES.SIEM_SITE_FK.in(Constants.select(SITES.SITE_PK, selectSites(site_pk, dept_pk, trng_pk != null))));

		if (trng_pk != null) {
			if (!this.restrictions.canAccessTrainings()) {
				throw new ForbiddenException();
			}

			query.addJoin(TRAININGS_EMPLOYEES, TRAININGS_EMPLOYEES.TREM_EMPL_FK.eq(EMPLOYEES.EMPL_PK), TRAININGS_EMPLOYEES.TREM_TRNG_FK.eq(trng_pk));
		}

		if (empl_pk != null) {
			query.addConditions(EMPLOYEES.EMPL_PK.eq(empl_pk));
		}

		return query;
	}

	@GET
	@Path("employees")
	public Result<? extends Record> listEmployees(
													@QueryParam("employee") final String empl_pk,
													@QueryParam("site") final String site_pk,
													@QueryParam("department") final Integer dept_pk,
													@QueryParam("training") final Integer trng_pk,
													@QueryParam("date") final String dateStr,
													@QueryParam("fields") final String fields) {
		try (final SelectQuery<? extends Record> query = selectEmployees(empl_pk, site_pk, dept_pk, trng_pk, dateStr)) {
			if (Constants.FIELDS_ALL.equals(fields)) {
				query.addSelect(EMPLOYEES.fields());
				query.addSelect(SITES_EMPLOYEES.fields());
			} else {
				query.addSelect(
								EMPLOYEES.EMPL_PK,
								EMPLOYEES.EMPL_FIRSTNAME,
								EMPLOYEES.EMPL_SURNAME,
								EMPLOYEES.EMPL_GENDER,
								EMPLOYEES.EMPL_PERMANENT,
								SITES_EMPLOYEES.SIEM_SITE_FK);
			}

			if (trng_pk != null) {
				query.addSelect(TRAININGS_EMPLOYEES.fields());
			}

			return this.ctx.fetch(query);
		}
	}

	@GET
	@Path("employees/{empl_pk}")
	public Record lookupEmployee(@PathParam("empl_pk") final String empl_pk, @QueryParam("date") final String dateStr) {
		return listEmployees(empl_pk, null, null, null, dateStr, Constants.FIELDS_ALL).get(0);
	}

	@GET
	@Path("employees/{empl_pk}/voiding")
	// TODO: /voiding -> /voidings
	// TODO: merge with employees listing/lookup?
	public Map<Integer, EmployeesCertificatesOptoutRecord> getEmployeeVoiding(@PathParam("empl_pk") final String empl_pk) {
		if (!this.restrictions.canAccessEmployee(empl_pk)) {
			throw new ForbiddenException();
		}

		return this.ctx
				.selectFrom(EMPLOYEES_CERTIFICATES_OPTOUT).where(EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_EMPL_FK.eq(empl_pk))
				.fetchMap(EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_CERT_FK);
	}

	public SelectQuery<Record> selectSites(final String site_pk, final Integer dept_pk, final boolean unlisted) {
		final SelectQuery<Record> query = DSL.select().getQuery();
		query.addFrom(SITES);
		if (site_pk != null) {
			if (!this.restrictions.canAccessSite(site_pk)) {
				throw new ForbiddenException();
			}

			query.addConditions(SITES.SITE_PK.eq(site_pk));
		}

		if (dept_pk != null) {
			if (!this.restrictions.canAccessDepartment(dept_pk)) {
				throw new ForbiddenException();
			}

			query.addConditions(SITES.SITE_DEPT_FK.eq(dept_pk));
		}

		if ((site_pk == null) && (dept_pk == null) && !this.restrictions.canAccessAllSites()) {
			query.addConditions(SITES.SITE_PK.in(this.restrictions.getAccessibleSites()));
		}

		if (!unlisted || !this.restrictions.canAccessAllSites()) {
			query.addConditions(SITES.SITE_PK.ne(Constants.UNASSIGNED_SITE));
		}

		return query;
	}

	@GET
	@Path("sites")
	public Result<Record> listSites(
									@QueryParam("site") final String site_pk,
									@QueryParam("department") final Integer dept_pk,
									@QueryParam("date") final String dateStr,
									@QueryParam("unlisted") final boolean unlisted) {

		try (final SelectQuery<Record> query = selectSites(site_pk, dept_pk, unlisted)) {
			query.addSelect(SITES.fields());
			query.addSelect(DSL.count(SITES_EMPLOYEES.SIEM_EMPL_FK).as("count"));
			query.addSelect(DSL.count(SITES_EMPLOYEES.SIEM_EMPL_FK).filterWhere(EMPLOYEES.EMPL_PERMANENT.eq(Boolean.TRUE)).as("permanent"));
			query.addJoin(
							SITES_EMPLOYEES.join(EMPLOYEES).on(EMPLOYEES.EMPL_PK.eq(SITES_EMPLOYEES.SIEM_EMPL_FK)),
							unlisted ? JoinType.LEFT_OUTER_JOIN : JoinType.JOIN,
							SITES_EMPLOYEES.SIEM_SITE_FK.eq(SITES.SITE_PK).and(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(Constants.selectUpdate(dateStr))));
			query.addGroupBy(SITES.fields());
			return this.ctx.fetch(query);
		}
	}

	@GET
	@Path("sites/{site_pk}")
	public Record lookupSite(
								@PathParam("site_pk") final String site_pk,
								@QueryParam("date") final String dateStr) {
		return listSites(site_pk, null, dateStr, true).get(0);
	}

	// TODO: REWRITE EVERYTHING BELOW

	@GET
	@Path("departments")
	public Result<? extends Record> listDepartments(@QueryParam("site") final String site_pk, @QueryParam("unlisted") final boolean unlisted) {
		if (!this.restrictions.canAccessAllSites() && (this.restrictions.getAccessibleDepartment() == null)) {
			throw new ForbiddenException();
		}

		if ((site_pk != null) && !this.restrictions.canAccessAllSites() && !this.restrictions.getAccessibleSites().contains(site_pk)) {
			throw new ForbiddenException();
		}

		try (
				final SelectQuery<Record> query = this.ctx.selectQuery();
				final Select<? extends Record> employees = DSL
						.select(SITES_EMPLOYEES.SIEM_SITE_FK).from(SITES_EMPLOYEES)
						.where(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(Constants.LATEST_UPDATE))) {
			query.addSelect(DEPARTMENTS.fields());
			query.addSelect(DSL.count(SITES.SITE_PK));
			query.addFrom(DEPARTMENTS);
			query.addGroupBy(DEPARTMENTS.fields());
			query.addJoin(SITES, unlisted ? JoinType.LEFT_OUTER_JOIN : JoinType.JOIN, SITES.SITE_DEPT_FK.eq(DEPARTMENTS.DEPT_PK));
			if (site_pk != null) {
				query.addConditions(DEPARTMENTS.DEPT_PK.eq(DSL.select(SITES.SITE_DEPT_FK).from(SITES).where(SITES.SITE_PK.eq(site_pk))));
			}

			if (!unlisted) {
				query.addJoin(employees, JoinType.JOIN, employees.field(SITES_EMPLOYEES.SIEM_SITE_FK).eq(SITES.SITE_PK));
			}

			if (!this.restrictions.canAccessAllSites()) {
				query.addConditions(DEPARTMENTS.DEPT_PK.eq(this.restrictions.getAccessibleDepartment()));
			}

			query.addConditions(DEPARTMENTS.DEPT_PK.ne(DSL.val(Constants.UNASSIGNED_DEPT)));
			return query.fetch();
		}
	}

	@GET
	@Path("departments/{dept_pk}")
	public Record lookupDepartment(@PathParam("dept_pk") final Integer dept_pk) {
		if (!this.restrictions.canAccessAllSites() && !this.restrictions.canAccessDepartment(dept_pk)) {
			throw new ForbiddenException();
		}

		return this.ctx.selectFrom(DEPARTMENTS).where(DEPARTMENTS.DEPT_PK.eq(dept_pk)).fetchOne();
	}

	@GET
	@Path("trainings")
	public Result<Record> listTrainings(
										@QueryParam("employee") final String empl_pk,
										@QueryParam("type") final List<Integer> types,
										@QueryParam("date") final String dateStr,
										@QueryParam("from") final String fromStr,
										@QueryParam("to") final String toStr,
										@QueryParam("completed") final Boolean completedOnly)
			throws ParseException {
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
	@Path("updates")
	// TODO: move to UpdateEndpoint?
	public Result<UpdatesRecord> listUpdates() {
		return this.ctx.selectFrom(UPDATES).orderBy(UPDATES.UPDT_DATE.desc()).fetch();
	}

	@GET
	@Path("updates/{date}")
	// TODO: move to UpdateEndpoint?
	public Record getUpdateFor(@PathParam("date") final String dateStr) {
		return this.ctx.selectFrom(UPDATES).where(UPDATES.UPDT_PK.eq(Constants.selectUpdate(dateStr))).fetchAny();
	}
}
