package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.CERTIFICATES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGTYPES_CERTIFICATES;

import java.math.BigDecimal;
import java.sql.Date;
import java.text.ParseException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.ccjmne.faomaintenance.api.modules.ResourcesUnrestricted;
import org.ccjmne.faomaintenance.api.modules.Restrictions;
import org.ccjmne.faomaintenance.api.rest.resources.TrainingsStatistics;
import org.ccjmne.faomaintenance.api.rest.resources.TrainingsStatistics.TrainingsStatisticsBuilder;
import org.ccjmne.faomaintenance.api.utils.Constants;
import org.ccjmne.faomaintenance.api.utils.SafeDateFormat;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record5;
import org.jooq.Record8;
import org.jooq.Record9;
import org.jooq.RecordMapper;
import org.jooq.Table;
import org.jooq.impl.DSL;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Range;

@Path("statistics")
public class StatisticsEndpoint {

	private static final Integer DEFAULT_INTERVAL = Integer.valueOf(6);

	private final DSLContext ctx;
	private final ResourcesEndpoint resources;
	private final ResourcesByKeysCommonEndpoint commonResources;

	// TODO: remove and delegate it all to ResourcesEndpoint
	private final Restrictions restrictions;

	@Inject
	public StatisticsEndpoint(
								final DSLContext ctx,
								final ResourcesEndpoint resources,
								final ResourcesByKeysCommonEndpoint resourcesByKeys,
								final ResourcesUnrestricted resourcesUnrestricted,
								final Restrictions restrictions) {
		this.ctx = ctx;
		this.resources = resources;
		this.commonResources = resourcesByKeys;
		this.restrictions = restrictions;
	}

	@GET
	@Path("trainings")
	// TODO: rewrite
	public Map<Integer, Iterable<TrainingsStatistics>> getTrainingsStats(
																			@QueryParam("from") final String fromStr,
																			@QueryParam("to") final String toStr,
																			@QueryParam("interval") final List<Integer> intervals)
			throws ParseException {
		if (!this.restrictions.canAccessTrainings()) {
			throw new ForbiddenException();
		}

		final List<Record> trainings = this.resources.listTrainings(null, Collections.EMPTY_LIST, null, null, null, Boolean.TRUE);
		final List<Record> trainingsByExpiry = this.resources.listTrainings(null, Collections.EMPTY_LIST, null, null, null, Boolean.TRUE)
				.sortAsc(Constants.TRAINING_EXPIRY);

		final Map<Integer, List<Integer>> certs = this.commonResources.listTrainingtypesCertificates();
		final Map<Integer, Iterable<TrainingsStatistics>> res = new HashMap<>();

		for (final Integer interval : intervals) {
			final List<TrainingsStatisticsBuilder> trainingsStatsBuilders = new ArrayList<>();

			computeDates(fromStr, toStr, interval).stream().reduce((cur, next) -> {
				trainingsStatsBuilders.add(TrainingsStatistics.builder(certs, Range.<Date> closedOpen(cur, next)));
				return next;
			});

			// [from, from+i[,
			// [from+i, from+2i[,
			// [from+2i, from+3i[,
			// ...
			// [to-i, to] <- closed
			trainingsStatsBuilders.get(trainingsStatsBuilders.size() - 1).closeRange();

			populateTrainingsStatsBuilders(
											trainingsStatsBuilders,
											trainings,
											(training) -> training.getValue(TRAININGS.TRNG_DATE),
											(builder, training) -> builder.registerTraining(training));
			populateTrainingsStatsBuilders(
											trainingsStatsBuilders,
											trainingsByExpiry,
											(record) -> record.getValue(Constants.TRAINING_EXPIRY),
											(builder, training) -> builder.registerExpiry(training));
			res.put(interval, trainingsStatsBuilders.stream().map(TrainingsStatisticsBuilder::build).collect(Collectors.toList()));
		}

		return res;
	}

	@GET
	@Path("departments/{dept_pk}")
	@SuppressWarnings("null")
	public Map<Integer, Record8<Integer, BigDecimal, BigDecimal, BigDecimal, Integer, Integer, Integer, BigDecimal>> getDepartmentStats(
																																		@PathParam("dept_pk") final Integer dept_pk,
																																		@QueryParam("date") final String dateStr) {
		final Table<Record8<Integer, String, Integer, Integer, Integer, Integer, Integer, String>> sitesStats = Constants
				.selectSitesStats(
									dateStr,
									TRAININGS_EMPLOYEES.TREM_EMPL_FK
											.in(Constants.select(EMPLOYEES.EMPL_PK, this.resources.selectEmployees(null, null, dept_pk, null, dateStr))),
									SITES_EMPLOYEES.SIEM_SITE_FK.in(Constants.select(SITES.SITE_PK, this.resources.selectSites(null, dept_pk, false))))
				.asTable();

		return this.ctx.select(
								sitesStats.field(CERTIFICATES.CERT_PK),
								DSL.sum(sitesStats.field(Constants.STATUS_SUCCESS, Integer.class)).as(Constants.STATUS_SUCCESS),
								DSL.sum(sitesStats.field(Constants.STATUS_WARNING, Integer.class)).as(Constants.STATUS_WARNING),
								DSL.sum(sitesStats.field(Constants.STATUS_DANGER, Integer.class)).as(Constants.STATUS_DANGER),
								DSL.count().filterWhere(sitesStats.field("validity", String.class).eq(Constants.STATUS_SUCCESS))
										.as("sites_" + Constants.STATUS_SUCCESS),
								DSL.count().filterWhere(sitesStats.field("validity", String.class).eq(Constants.STATUS_WARNING))
										.as("sites_" + Constants.STATUS_WARNING),
								DSL.count().filterWhere(sitesStats.field("validity", String.class).eq(Constants.STATUS_DANGER))
										.as("sites_" + Constants.STATUS_DANGER),
								DSL.round(DSL.sum(DSL
										.when(sitesStats.field("validity", String.class).eq(Constants.STATUS_SUCCESS), DSL.val(1f))
										.when(sitesStats.field("validity", String.class).eq(Constants.STATUS_WARNING), DSL.val(2 / 3f))
										.otherwise(DSL.val(0f))).mul(DSL.val(100)).div(DSL.count())).as("score"))
				.from(sitesStats)
				.groupBy(sitesStats.field(CERTIFICATES.CERT_PK))
				.fetchMap(sitesStats.field(CERTIFICATES.CERT_PK));
	}

	@GET
	@Path("departments")
	public Map<Integer, Map<Integer, Object>> getDepartmentsStats(@QueryParam("date") final String dateStr) {
		// TODO: implement and use resources#selectDepartments();
		final Table<Record9<Integer, Integer, BigDecimal, BigDecimal, BigDecimal, Integer, Integer, Integer, BigDecimal>> departmentsStats = Constants
				.selectDepartmentsStats(
										dateStr,
										TRAININGS_EMPLOYEES.TREM_EMPL_FK
												.in(Constants.select(EMPLOYEES.EMPL_PK, this.resources.selectEmployees(null, null, null, null, dateStr))),
										SITES_EMPLOYEES.SIEM_SITE_FK.in(Constants.select(SITES.SITE_PK, this.resources.selectSites(null, null, false))),
										null)
				.asTable();

		return this.ctx.select(
								departmentsStats.field(SITES.SITE_DEPT_FK),
								DSL.arrayAgg(departmentsStats.field(CERTIFICATES.CERT_PK)).as("cert_pk"),
								DSL.arrayAgg(departmentsStats.field(Constants.STATUS_SUCCESS)).as(Constants.STATUS_SUCCESS),
								DSL.arrayAgg(departmentsStats.field(Constants.STATUS_WARNING)).as(Constants.STATUS_WARNING),
								DSL.arrayAgg(departmentsStats.field(Constants.STATUS_DANGER)).as(Constants.STATUS_DANGER),
								DSL.arrayAgg(departmentsStats.field("sites_" +
										Constants.STATUS_SUCCESS)).as("sites_" + Constants.STATUS_SUCCESS),
								DSL.arrayAgg(departmentsStats.field("sites_" +
										Constants.STATUS_WARNING)).as("sites_" + Constants.STATUS_WARNING),
								DSL.arrayAgg(departmentsStats.field("sites_" +
										Constants.STATUS_DANGER)).as("sites_" + Constants.STATUS_DANGER),
								DSL.arrayAgg(departmentsStats.field("score")).as("score"))
				.from(departmentsStats)
				.groupBy(departmentsStats.field(SITES.SITE_DEPT_FK))
				.fetchMap(departmentsStats.field(SITES.SITE_DEPT_FK), getZipMapper(
																					"cert_pk",
																					Constants.STATUS_SUCCESS,
																					Constants.STATUS_WARNING,
																					Constants.STATUS_DANGER,
																					"sites_" + Constants.STATUS_SUCCESS,
																					"sites_" + Constants.STATUS_WARNING,
																					"sites_" + Constants.STATUS_DANGER,
																					"score"));
	}

	@GET
	@Path("sites/{site_pk}")
	public Map<Integer, Record8<Integer, String, Integer, Integer, Integer, Integer, Integer, String>> getSiteStats(
																													@PathParam("site_pk") final String site_pk,
																													@QueryParam("date") final String dateStr) {
		return this.ctx.selectQuery(Constants
				.selectSitesStats(
									dateStr,
									TRAININGS_EMPLOYEES.TREM_EMPL_FK
											.in(Constants.select(EMPLOYEES.EMPL_PK, this.resources.selectEmployees(null, site_pk, null, null, dateStr))),
									SITES_EMPLOYEES.SIEM_SITE_FK.in(Constants.select(SITES.SITE_PK, this.resources.selectSites(site_pk, null, false)))))
				.fetchMap(CERTIFICATES.CERT_PK);
	}

	@GET
	@Path("sites")
	public Map<String, Map<Integer, Object>> getSitesStats(
															@QueryParam("department") final Integer dept_pk,
															@QueryParam("date") final String dateStr) {

		final Table<Record8<Integer, String, Integer, Integer, Integer, Integer, Integer, String>> sitesStats = Constants
				.selectSitesStats(
									dateStr,
									TRAININGS_EMPLOYEES.TREM_EMPL_FK
											.in(Constants.select(EMPLOYEES.EMPL_PK, this.resources.selectEmployees(null, null, dept_pk, null, dateStr))),
									SITES_EMPLOYEES.SIEM_SITE_FK.in(Constants.select(SITES.SITE_PK, this.resources.selectSites(null, dept_pk, false))))
				.asTable();

		return this.ctx.select(
								sitesStats.field(SITES_EMPLOYEES.SIEM_SITE_FK),
								DSL.arrayAgg(sitesStats.field(CERTIFICATES.CERT_PK)).as("cert_pk"),
								DSL.arrayAgg(sitesStats.field(Constants.STATUS_SUCCESS)).as(Constants.STATUS_SUCCESS),
								DSL.arrayAgg(sitesStats.field(Constants.STATUS_WARNING)).as(Constants.STATUS_WARNING),
								DSL.arrayAgg(sitesStats.field(Constants.STATUS_DANGER)).as(Constants.STATUS_DANGER),
								DSL.arrayAgg(sitesStats.field("target")).as("target"),
								DSL.arrayAgg(sitesStats.field("validity")).as("validity"))
				.from(sitesStats)
				.groupBy(sitesStats.field(SITES_EMPLOYEES.SIEM_SITE_FK))
				.fetchMap(
							SITES_EMPLOYEES.SIEM_SITE_FK,
							getZipMapper("cert_pk", Constants.STATUS_SUCCESS, Constants.STATUS_WARNING, Constants.STATUS_DANGER, "target", "validity"));
	}

	@GET
	@Path("employees/{empl_pk}")
	public Map<Integer, Record5<String, String, Integer, Date, String>> getEmployeeStats(
																							@PathParam("empl_pk") final String empl_pk,
																							@QueryParam("date") final String dateStr) {
		return this.ctx.selectQuery(Constants
				.selectEmployeesStats(dateStr, TRAININGS_EMPLOYEES.TREM_EMPL_FK.eq(empl_pk)))
				.fetchMap(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK);
	}

	@GET
	@Path("employees")
	public Map<String, Map<Integer, Object>> getEmployeesStats(
																@QueryParam("site") final String site_pk,
																@QueryParam("department") final Integer dept_pk,
																@QueryParam("date") final String dateStr) {

		final Table<Record5<String, String, Integer, Date, String>> employeesStats = Constants
				.selectEmployeesStats(
										dateStr,
										TRAININGS_EMPLOYEES.TREM_EMPL_FK
												.in(Constants.select(EMPLOYEES.EMPL_PK, this.resources.selectEmployees(null, site_pk, dept_pk, null, dateStr))))
				.asTable();

		return this.ctx
				.select(
						employeesStats.field(TRAININGS_EMPLOYEES.TREM_EMPL_FK),
						DSL.arrayAgg(employeesStats.field(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK)).as("cert_pk"),
						DSL.arrayAgg(employeesStats.field("expiry")).as("expiry"),
						DSL.arrayAgg(employeesStats.field("validity")).as("validity"))
				.from(employeesStats)
				.groupBy(employeesStats.field(TRAININGS_EMPLOYEES.TREM_EMPL_FK))
				.fetchMap(employeesStats.field(TRAININGS_EMPLOYEES.TREM_EMPL_FK), getZipMapper("cert_pk", "expiry", "validity"));
	}

	private static RecordMapper<Record, Map<Integer, Object>> getZipMapper(final String key, final String... fields) {
		return new RecordMapper<Record, Map<Integer, Object>>() {

			@Override
			public Map<Integer, Object> map(final Record record) {
				final Map<Integer, Object> res = new HashMap<>();
				final Integer[] keys = (Integer[]) record.get(key);
				for (int i = 0; i < keys.length; i++) {
					final int idx = i;
					final Builder<String, Object> builder = ImmutableMap.<String, Object> builder();
					Arrays.asList(fields).forEach(field -> builder.put(field, ((Object[]) record.get(field))[idx]));
					res.put(keys[i], builder.build());
				}

				return res;
			}
		};
	}

	private static void populateTrainingsStatsBuilders(
														final List<TrainingsStatisticsBuilder> trainingsStatsBuilders,
														final Iterable<Record> trainings,
														final Function<Record, Date> dateMapper,
														final BiConsumer<TrainingsStatisticsBuilder, Record> populateFunction) {
		final Iterator<TrainingsStatisticsBuilder> iterator = trainingsStatsBuilders.iterator();
		TrainingsStatisticsBuilder next = iterator.next();
		for (final Record training : trainings) {
			final Date relevantDate = dateMapper.apply(training);
			if (relevantDate.before(next.getBeginning())) {
				continue;
			}

			while (!next.getDateRange().contains(relevantDate) && iterator.hasNext()) {
				next = iterator.next();
			}

			if (next.getDateRange().contains(relevantDate)) {
				populateFunction.accept(next, training);
			}
		}
	}

	private static List<Date> computeDates(final String fromStr, final String toStr, final Integer intervalRaw) throws ParseException {
		final Date utmost = (toStr == null) ? new Date(new java.util.Date().getTime()) : SafeDateFormat.parseAsSql(toStr);
		if (fromStr == null) {
			return Collections.singletonList(utmost);
		}

		final int interval = (intervalRaw != null ? intervalRaw : DEFAULT_INTERVAL).intValue();
		LocalDate cur = SafeDateFormat.parseAsSql(fromStr).toLocalDate();
		if (interval == 0) {
			return ImmutableList.<Date> of(Date.valueOf(cur), utmost);
		}

		final List<Date> res = new ArrayList<>();
		do {
			res.add(Date.valueOf(cur));
			cur = cur.plusMonths(interval);
		} while (cur.isBefore(utmost.toLocalDate()));

		res.add(utmost);
		return res;
	}
}
