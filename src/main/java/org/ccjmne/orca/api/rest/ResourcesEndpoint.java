package org.ccjmne.orca.api.rest;

import static org.ccjmne.orca.jooq.classes.Tables.DEPARTMENTS;
import static org.ccjmne.orca.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.EMPLOYEES_VOIDINGS;
import static org.ccjmne.orca.jooq.classes.Tables.SITES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES_TAGS;
import static org.ccjmne.orca.jooq.classes.Tables.TAGS;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGTYPES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGTYPES_CERTIFICATES;
import static org.ccjmne.orca.jooq.classes.Tables.UPDATES;

import java.sql.Date;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import org.ccjmne.orca.api.modules.Restrictions;
import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.api.utils.Constants.RecordSlicer;
import org.ccjmne.orca.api.utils.RestrictedResourcesHelper;
import org.ccjmne.orca.api.utils.SafeDateFormat;
import org.ccjmne.orca.api.utils.StatisticsHelper;
import org.ccjmne.orca.api.utils.UnrestrictedResourcesHelper;
import org.ccjmne.orca.jooq.classes.tables.records.TrainingsEmployeesRecord;
import org.ccjmne.orca.jooq.classes.tables.records.UpdatesRecord;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JoinType;
import org.jooq.Record;
import org.jooq.RecordMapper;
import org.jooq.Result;
import org.jooq.SelectQuery;
import org.jooq.Table;
import org.jooq.impl.DSL;

@Path("resources")
public class ResourcesEndpoint {

	private final DSLContext ctx;
	private final RestrictedResourcesHelper resourcesHelper;

	// TODO: Should not have any use for this and should delegate restricted
	// data access mechanics to RestrictedResourcesHelper
	private final Restrictions restrictions;

	@Inject
	public ResourcesEndpoint(final DSLContext ctx, final Restrictions restrictions, final RestrictedResourcesHelper resourcesHelper) {
		this.ctx = ctx;
		this.restrictions = restrictions;
		this.resourcesHelper = resourcesHelper;
	}

	@GET
	@Path("employees")
	public List<Map<String, Object>> listEmployees(
													@QueryParam("employee") final String empl_pk,
													@QueryParam("site") final String site_pk,
													@QueryParam("department") final Integer dept_pk,
													@QueryParam("training") final Integer trng_pk,
													@QueryParam("date") final String dateStr,
													@QueryParam("fields") final String fields) {
		try (final SelectQuery<? extends Record> query = this.resourcesHelper.selectEmployees(empl_pk, site_pk, dept_pk, trng_pk, dateStr)) {
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

			final List<Field<?>> selected = new ArrayList<>(query.getSelect());
			query.addSelect(Constants.arrayAgg(EMPLOYEES_VOIDINGS.EMVO_CERT_FK),
							Constants.arrayAgg(EMPLOYEES_VOIDINGS.EMVO_DATE),
							Constants.arrayAgg(EMPLOYEES_VOIDINGS.EMVO_REASON));
			query.addJoin(EMPLOYEES_VOIDINGS, JoinType.LEFT_OUTER_JOIN, EMPLOYEES_VOIDINGS.EMVO_EMPL_FK.eq(EMPLOYEES.EMPL_PK));
			query.addGroupBy(selected);

			return this.ctx.fetch(query).map(new RecordMapper<Record, Map<String, Object>>() {

				private final RecordMapper<Record, Map<Integer, Object>> zipMapper = Constants
						.getZipMapper(EMPLOYEES_VOIDINGS.EMVO_CERT_FK, EMPLOYEES_VOIDINGS.EMVO_DATE, EMPLOYEES_VOIDINGS.EMVO_REASON);

				@Override
				public Map<String, Object> map(final Record record) {
					final Map<String, Object> res = new HashMap<>();
					selected.forEach(field -> res.put(field.getName(), record.get(field)));
					final Map<Integer, Object> map = this.zipMapper.map(record);
					if (!map.isEmpty()) {
						res.put("voidings", map);
					}

					return res;
				}
			});
		}
	}

	/**
	 * Used in order to load all training sessions outcomes for the employees'
	 * advanced search module.
	 */
	// TODO: Restrict this method (and accordingly: the corresponding options in
	// the advanced search module) to users who can access training sessions?
	@GET
	@Path("employees/trainings")
	public Map<String, Result<Record>> listEmployeesTrainings(
																@QueryParam("employee") final String empl_pk,
																@QueryParam("site") final String site_pk,
																@QueryParam("department") final Integer dept_pk,
																@QueryParam("training") final Integer trng_pk,
																@QueryParam("date") final String dateStr) {
		return this.ctx
				.select(TRAININGS_EMPLOYEES.TREM_EMPL_FK, TRAININGS_EMPLOYEES.TREM_OUTCOME, TRAININGS.TRNG_DATE)
				.select(DSL.arrayAgg(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK).as("certificates"))
				.from(TRAININGS_EMPLOYEES)
				.join(TRAININGS).on(TRAININGS.TRNG_PK.eq(TRAININGS_EMPLOYEES.TREM_TRNG_FK))
				.join(TRAININGTYPES_CERTIFICATES).on(TRAININGTYPES_CERTIFICATES.TTCE_TRTY_FK.eq(TRAININGS.TRNG_TRTY_FK))
				.where(TRAININGS_EMPLOYEES.TREM_EMPL_FK
						.in(Constants.select(EMPLOYEES.EMPL_PK, this.resourcesHelper.selectEmployees(empl_pk, site_pk, dept_pk, trng_pk, dateStr))))
				.groupBy(TRAININGS_EMPLOYEES.TREM_EMPL_FK, TRAININGS_EMPLOYEES.TREM_OUTCOME, TRAININGS.TRNG_DATE)
				.fetchGroups(TRAININGS_EMPLOYEES.TREM_EMPL_FK);
	}

	@GET
	@Path("employees/{empl_pk}")
	public Map<String, Object> lookupEmployee(@PathParam("empl_pk") final String empl_pk, @QueryParam("date") final String dateStr) {
		try {
			return listEmployees(empl_pk, null, null, null, dateStr, Constants.FIELDS_ALL).get(0);
		} catch (final IndexOutOfBoundsException e) {
			throw new NotFoundException();
		}
	}

	/**
	 * @param site_pk
	 *            If specified, limits selection to the site uniquely identified
	 *            by that.
	 * @param dateStr
	 *            If specified, uses the most relevant employees-sites
	 *            assignment as of that date; otherwise, uses that of now.
	 * @param unlisted
	 *            If <code>true</code>, doesn't skip sites with no employees
	 *            assigned.
	 * @param uriInfo
	 *            Used to obtain a map of supplied tag filters to select sites
	 *            using
	 *            {@link RestrictedResourcesHelper#selectSitesByTags(String, Map)}
	 */
	@GET
	@Path("sites")
	public List<Map<String, Object>> listSites(
												@QueryParam("site") final String site_pk,
												@QueryParam("date") final String dateStr,
												@QueryParam("unlisted") final boolean unlisted,
												@Context final UriInfo uriInfo) {
		// Only integer values are valid tag keys
		final Map<Integer, List<String>> filters = uriInfo.getQueryParameters().entrySet().stream()
				.filter(x -> x.getKey().matches("\\d+"))
				.collect(Collectors.<Entry<String, List<String>>, Integer, List<String>> toMap(x -> Integer.valueOf(x.getKey()), Entry::getValue));

		return listSitesImpl(site_pk, dateStr, unlisted, filters);
	}

	@GET
	@Path("sites/{site_pk}")
	public Map<String, Object> lookupSite(
											@PathParam("site_pk") final String site_pk,
											@QueryParam("date") final String dateStr,
											@QueryParam("unlisted") final boolean unlisted) {
		try {
			return listSitesImpl(site_pk, dateStr, unlisted, Collections.<Integer, List<String>> emptyMap()).get(0);
		} catch (final IndexOutOfBoundsException e) {
			throw new NotFoundException();
		}
	}

	@GET
	@Path("departments")
	public Result<? extends Record> listDepartments(
													@QueryParam("department") final Integer dept_pk,
													@QueryParam("date") final String dateStr,
													@QueryParam("unlisted") final boolean unlisted) {
		final Table<Record> counts = DSL.select(SITES_EMPLOYEES.SIEM_SITE_FK)
				.select(DSL.count(SITES_EMPLOYEES.SIEM_EMPL_FK).as("count"))
				.select(DSL.count(SITES_EMPLOYEES.SIEM_EMPL_FK).filterWhere(EMPLOYEES.EMPL_PERMANENT.eq(Boolean.TRUE)).as("permanent"))
				.from(SITES_EMPLOYEES.join(EMPLOYEES).on(EMPLOYEES.EMPL_PK.eq(SITES_EMPLOYEES.SIEM_EMPL_FK)))
				.where(SITES_EMPLOYEES.SIEM_SITE_FK.in(Constants.select(SITES.SITE_PK, this.resourcesHelper.selectSites(null, dept_pk))))
				.and(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(Constants.selectUpdate(dateStr)))
				.groupBy(SITES_EMPLOYEES.SIEM_SITE_FK).asTable();
		try (final SelectQuery<Record> query = this.resourcesHelper.selectDepartments(dept_pk)) {
			query.addSelect(DEPARTMENTS.fields());
			query.addSelect(DSL.sum(counts.field("count", Integer.class)).as("count"));
			query.addSelect(DSL.sum(counts.field("permanent", Integer.class)).as("permanent"));
			query.addSelect(DSL.count(SITES.SITE_PK).as("sites_count"));
			query.addJoin(
							SITES,
							unlisted ? JoinType.LEFT_OUTER_JOIN : JoinType.JOIN,
							SITES.SITE_DEPT_FK.eq(DEPARTMENTS.DEPT_PK));
			query.addJoin(
							counts,
							unlisted ? JoinType.LEFT_OUTER_JOIN : JoinType.JOIN,
							counts.field(SITES_EMPLOYEES.SIEM_SITE_FK).eq(SITES.SITE_PK));
			query.addGroupBy(DEPARTMENTS.fields());
			return this.ctx.fetch(query);
		}
	}

	@GET
	@Path("departments/{dept_pk}")
	public Record lookupDepartment(
									@PathParam("dept_pk") final Integer dept_pk,
									@QueryParam("date") final String dateStr,
									@QueryParam("unlisted") final boolean unlisted) {
		try {
			return listDepartments(dept_pk, dateStr, unlisted).get(0);
		} catch (final IndexOutOfBoundsException e) {
			throw new NotFoundException();
		}
	}

	// TODO: REWRITE EVERYTHING BELOW

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

			query.addSelect(StatisticsHelper.TRAINING_REGISTERED);
			query.addSelect(StatisticsHelper.TRAINING_VALIDATED);
			query.addSelect(StatisticsHelper.TRAINING_FLUNKED);
			query.addSelect(StatisticsHelper.TRAINING_MISSING);
			query.addSelect(StatisticsHelper.TRAINING_TRAINERS);

			if (empl_pk != null) {
				final Table<TrainingsEmployeesRecord> employeeOutcomes = DSL.selectFrom(TRAININGS_EMPLOYEES).where(TRAININGS_EMPLOYEES.TREM_EMPL_FK.eq(empl_pk))
						.asTable();
				query.addJoin(employeeOutcomes, employeeOutcomes.field(TRAININGS_EMPLOYEES.TREM_TRNG_FK).eq(TRAININGS_EMPLOYEES.TREM_TRNG_FK));
				query.addSelect(employeeOutcomes.fields());
				query.addGroupBy(employeeOutcomes.fields());
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
			query.addSelect(StatisticsHelper.TRAINING_REGISTERED);
			query.addSelect(StatisticsHelper.TRAINING_VALIDATED);
			query.addSelect(StatisticsHelper.TRAINING_FLUNKED);
			query.addSelect(StatisticsHelper.TRAINING_MISSING);
			query.addSelect(StatisticsHelper.TRAINING_TRAINERS);
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
	public Record lookupUpdate(@PathParam("date") final String dateStr) {
		return this.ctx.selectFrom(UPDATES).where(UPDATES.UPDT_PK.eq(Constants.selectUpdate(dateStr))).fetchAny();
	}

	private List<Map<String, Object>> listSitesImpl(
													final String site_pk,
													final String dateStr,
													final boolean unlisted,
													final Map<Integer, List<String>> filters) {
		try (final SelectQuery<Record> selectSites = this.resourcesHelper.selectSitesByTags(site_pk, filters)) {
			selectSites.addSelect(SITES.fields());
			selectSites.addSelect(DSL.count(SITES_EMPLOYEES.SIEM_EMPL_FK).as("count"));
			selectSites.addSelect(DSL.count(SITES_EMPLOYEES.SIEM_EMPL_FK).filterWhere(EMPLOYEES.EMPL_PERMANENT.eq(Boolean.TRUE)).as("permanent"));
			selectSites.addJoin(
								SITES_EMPLOYEES.join(EMPLOYEES).on(EMPLOYEES.EMPL_PK.eq(SITES_EMPLOYEES.SIEM_EMPL_FK)),
								unlisted ? JoinType.LEFT_OUTER_JOIN : JoinType.JOIN,
								SITES_EMPLOYEES.SIEM_SITE_FK.eq(SITES.SITE_PK).and(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(Constants.selectUpdate(dateStr))));
			selectSites.addGroupBy(SITES.fields());

			try (final SelectQuery<Record> withTags = DSL.select().getQuery()) {
				final Table<Record> sites = selectSites.asTable();
				withTags.addSelect(sites.fields());
				withTags.addSelect(	Constants.arrayAgg(TAGS.TAGS_TYPE),
									Constants.arrayAgg(SITES_TAGS.SITA_TAGS_FK),
									Constants.arrayAgg(SITES_TAGS.SITA_VALUE));
				withTags.addFrom(sites);
				withTags.addJoin(	SITES_TAGS.join(TAGS).on(TAGS.TAGS_PK.eq(SITES_TAGS.SITA_TAGS_FK)),
									JoinType.LEFT_OUTER_JOIN,
									SITES_TAGS.SITA_SITE_FK.eq(sites.field(SITES.SITE_PK)));
				withTags.addGroupBy(sites.fields());

				return this.ctx.fetch(withTags).map(new RecordMapper<Record, Map<String, Object>>() {

					private final BiFunction<RecordSlicer, ? super String, ? extends Object> coercer = (slicer, data) -> slicer
							.getSlice(TAGS.TAGS_TYPE).equals(Constants.TAGS_TYPE_BOOLEAN) ? Boolean.valueOf(data) : data;

					private final RecordMapper<Record, Map<Integer, Object>> selectMapper = Constants
							.getSelectMapper(this.coercer, SITES_TAGS.SITA_TAGS_FK, SITES_TAGS.SITA_VALUE);

					@Override
					public Map<String, Object> map(final Record record) {
						final Map<String, Object> res = new HashMap<>();
						selectSites.getSelect().forEach(field -> res.put(field.getName(), record.get(field)));
						final Map<Integer, Object> map = this.selectMapper.map(record);
						if (!map.isEmpty()) {
							res.put("tags", map);
						}

						return res;
					}
				});
			}
		}
	}
}
