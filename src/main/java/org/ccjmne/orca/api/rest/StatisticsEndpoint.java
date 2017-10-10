package org.ccjmne.orca.api.rest;

import static org.ccjmne.orca.api.utils.Constants.STATUS_DANGER;
import static org.ccjmne.orca.api.utils.Constants.STATUS_SUCCESS;
import static org.ccjmne.orca.api.utils.Constants.STATUS_WARNING;
import static org.ccjmne.orca.jooq.classes.Tables.CERTIFICATES;
import static org.ccjmne.orca.jooq.classes.Tables.DEPARTMENTS;
import static org.ccjmne.orca.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGTYPES_CERTIFICATES;

import java.math.BigDecimal;
import java.sql.Date;
import java.text.ParseException;
import java.time.LocalDate;
import java.util.ArrayList;
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

import org.ccjmne.orca.api.modules.Restrictions;
import org.ccjmne.orca.api.rest.resources.TrainingsStatistics;
import org.ccjmne.orca.api.rest.resources.TrainingsStatistics.TrainingsStatisticsBuilder;
import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.api.utils.RestrictedResourcesHelper;
import org.ccjmne.orca.api.utils.SafeDateFormat;
import org.ccjmne.orca.api.utils.StatisticsHelper;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record10;
import org.jooq.Record6;
import org.jooq.Record8;
import org.jooq.Table;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;

@Path("statistics")
public class StatisticsEndpoint {

	private static final Integer DEFAULT_INTERVAL = Integer.valueOf(1);

	private final DSLContext ctx;
	private final ResourcesCommonEndpoint commonResources;
	private final RestrictedResourcesHelper resourcesHelper;

	// TODO: Should not have any use for these two and should delegate
	// restricted data access mechanics to RestrictedResourcesHelper
	private final ResourcesEndpoint resources;
	private final Restrictions restrictions;

	@Inject
	public StatisticsEndpoint(
								final DSLContext ctx,
								final ResourcesEndpoint resources,
								final ResourcesCommonEndpoint commonResources,
								final RestrictedResourcesHelper resourcesHelper,
								final Restrictions restrictions) {
		this.ctx = ctx;
		this.resources = resources;
		this.commonResources = commonResources;
		this.resourcesHelper = resourcesHelper;
		this.restrictions = restrictions;
	}

	@GET
	@Path("trainings")
	// TODO: rewrite
	// TODO: The entire thing is close to being straight-up irrelevant
	// altogether.
	public Map<Integer, Iterable<TrainingsStatistics>> getTrainingsStats(
																			@QueryParam("from") final String fromStr,
																			@QueryParam("to") final String toStr,
																			@QueryParam("interval") final List<Integer> intervals)
			throws ParseException {
		if (!this.restrictions.canAccessTrainings()) {
			throw new ForbiddenException();
		}

		final List<Record> trainings = this.resources.listTrainings(null, Collections.EMPTY_LIST, null, null, null, Boolean.TRUE);

		final Map<Integer, List<Integer>> certs = this.commonResources.listTrainingTypesCertificates()
				.intoGroups(TRAININGTYPES_CERTIFICATES.TTCE_TRTY_FK, TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK);
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
			res.put(interval, trainingsStatsBuilders.stream().map(TrainingsStatisticsBuilder::build).collect(Collectors.toList()));
		}

		return res;
	}

	@GET
	@Path("departments/{dept_pk}")
	public Map<Integer, Record10<Integer, Integer, BigDecimal, BigDecimal, BigDecimal, Integer, Integer, Integer, BigDecimal, String>> getDepartmentStats(
																																							@PathParam("dept_pk") final Integer dept_pk,
																																							@QueryParam("date") final String dateStr) {
		return this.ctx.selectQuery(StatisticsHelper
				.selectDepartmentsStats(
										dateStr,
										TRAININGS_EMPLOYEES.TREM_EMPL_FK
												.in(Constants.select(	EMPLOYEES.EMPL_PK,
																		this.resourcesHelper.selectEmployees(null, null, dept_pk, null, dateStr))),
										SITES_EMPLOYEES.SIEM_SITE_FK.in(Constants.select(SITES.SITE_PK, this.resourcesHelper.selectSites(null, dept_pk))),
										DEPARTMENTS.DEPT_PK.in(Constants.select(DEPARTMENTS.DEPT_PK, this.resourcesHelper.selectDepartments(dept_pk)))))
				.fetchMap(CERTIFICATES.CERT_PK);
	}

	@GET
	@Path("departments/{dept_pk}/history")
	public Map<Object, Object> getDepartmentStatsHistory(
															@PathParam("dept_pk") final Integer dept_pk,
															@QueryParam("from") final String from,
															@QueryParam("to") final String to,
															@QueryParam("interval") final Integer interval)
			throws ParseException {
		return StatisticsEndpoint.computeDates(from, to, interval).stream()
				.map(date -> Collections.singletonMap(date, getDepartmentStats(dept_pk, date.toString())))
				.reduce(ImmutableMap.<Object, Object> builder(), (res, entry) -> res.putAll(entry), (m1, m2) -> m1.putAll(m2.build())).build();
	}

	@GET
	@Path("departments")
	public Map<Integer, Map<Integer, Object>> getDepartmentsStats(@QueryParam("date") final String dateStr) {
		final Table<Record10<Integer, Integer, BigDecimal, BigDecimal, BigDecimal, Integer, Integer, Integer, BigDecimal, String>> departmentsStats = StatisticsHelper
				.selectDepartmentsStats(
										dateStr,
										TRAININGS_EMPLOYEES.TREM_EMPL_FK
												.in(Constants.select(EMPLOYEES.EMPL_PK, this.resourcesHelper.selectEmployees(null, null, null, null, dateStr))),
										SITES_EMPLOYEES.SIEM_SITE_FK.in(Constants.select(SITES.SITE_PK, this.resourcesHelper.selectSites(null, null))),
										DEPARTMENTS.DEPT_PK.in(Constants.select(DEPARTMENTS.DEPT_PK, this.resourcesHelper.selectDepartments(null))))
				.asTable();

		return this.ctx.select(
								departmentsStats.field(DEPARTMENTS.DEPT_PK),
								Constants.arrayAgg(departmentsStats.field(CERTIFICATES.CERT_PK)).as("cert_pk"),
								Constants.arrayAgg(departmentsStats.field(STATUS_SUCCESS)),
								Constants.arrayAgg(departmentsStats.field(STATUS_WARNING)),
								Constants.arrayAgg(departmentsStats.field(STATUS_DANGER)),
								Constants.arrayAgg(departmentsStats.field("sites_" + STATUS_SUCCESS)),
								Constants.arrayAgg(departmentsStats.field("sites_" + STATUS_WARNING)),
								Constants.arrayAgg(departmentsStats.field("sites_" + STATUS_DANGER)),
								Constants.arrayAgg(departmentsStats.field("score")),
								Constants.arrayAgg(departmentsStats.field("validity")))
				.from(departmentsStats)
				.groupBy(departmentsStats.field(DEPARTMENTS.DEPT_PK))
				.fetchMap(
							departmentsStats.field(DEPARTMENTS.DEPT_PK),
							Constants.getZipMapper(	"cert_pk", STATUS_SUCCESS, STATUS_WARNING, STATUS_DANGER, "sites_" + STATUS_SUCCESS,
													"sites_" + STATUS_WARNING, "sites_" + STATUS_DANGER, "score", "validity"));
	}

	@GET
	@Path("sites/{site_pk}")
	public Map<Integer, Record8<Integer, String, Integer, Integer, Integer, Integer, Integer, String>> getSiteStats(
																													@PathParam("site_pk") final String site_pk,
																													@QueryParam("date") final String dateStr) {
		return this.ctx.selectQuery(StatisticsHelper
				.selectSitesStats(
									dateStr,
									TRAININGS_EMPLOYEES.TREM_EMPL_FK
											.in(Constants.select(EMPLOYEES.EMPL_PK, this.resourcesHelper.selectEmployees(null, site_pk, null, null, dateStr))),
									SITES_EMPLOYEES.SIEM_SITE_FK.in(Constants.select(SITES.SITE_PK, this.resourcesHelper.selectSites(site_pk, null)))))
				.fetchMap(CERTIFICATES.CERT_PK);
	}

	@GET
	@Path("sites/{site_pk}/history")
	public Map<Object, Object> getSiteStatsHistory(
													@PathParam("site_pk") final String site_pk,
													@QueryParam("from") final String from,
													@QueryParam("to") final String to,
													@QueryParam("interval") final Integer interval)
			throws ParseException {
		return StatisticsEndpoint.computeDates(from, to, interval).stream()
				.map(date -> Collections.singletonMap(date, getSiteStats(site_pk, date.toString())))
				.reduce(ImmutableMap.<Object, Object> builder(), (res, entry) -> res.putAll(entry), (m1, m2) -> m1.putAll(m2.build())).build();
	}

	@GET
	@Path("sites")
	public Map<String, Map<Integer, Object>> getSitesStats(
															@QueryParam("department") final Integer dept_pk,
															@QueryParam("date") final String dateStr) {

		final Table<Record8<Integer, String, Integer, Integer, Integer, Integer, Integer, String>> sitesStats = StatisticsHelper
				.selectSitesStats(
									dateStr,
									TRAININGS_EMPLOYEES.TREM_EMPL_FK
											.in(Constants.select(EMPLOYEES.EMPL_PK, this.resourcesHelper.selectEmployees(null, null, dept_pk, null, dateStr))),
									SITES_EMPLOYEES.SIEM_SITE_FK.in(Constants.select(SITES.SITE_PK, this.resourcesHelper.selectSites(null, dept_pk))))
				.asTable();

		return this.ctx.select(
								sitesStats.field(SITES_EMPLOYEES.SIEM_SITE_FK),
								Constants.arrayAgg(sitesStats.field(CERTIFICATES.CERT_PK)).as("cert_pk"),
								Constants.arrayAgg(sitesStats.field(STATUS_SUCCESS)),
								Constants.arrayAgg(sitesStats.field(STATUS_WARNING)),
								Constants.arrayAgg(sitesStats.field(STATUS_DANGER)),
								Constants.arrayAgg(sitesStats.field("target")),
								Constants.arrayAgg(sitesStats.field("validity")))
				.from(sitesStats)
				.groupBy(sitesStats.field(SITES_EMPLOYEES.SIEM_SITE_FK))
				.fetchMap(
							SITES_EMPLOYEES.SIEM_SITE_FK,
							Constants.getZipMapper("cert_pk", STATUS_SUCCESS, STATUS_WARNING, STATUS_DANGER, "target", "validity"));
	}

	@GET
	@Path("employees/{empl_pk}")
	public Map<Integer, Record6<String, String, Integer, Date, Date, String>> getEmployeeStats(
																								@PathParam("empl_pk") final String empl_pk,
																								@QueryParam("date") final String dateStr) {
		return this.ctx.selectQuery(StatisticsHelper
				.selectEmployeesStats(dateStr, TRAININGS_EMPLOYEES.TREM_EMPL_FK
						.in(Constants.select(EMPLOYEES.EMPL_PK, this.resourcesHelper.selectEmployees(empl_pk, null, null, null, dateStr)))))
				.fetchMap(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK);
	}

	@GET
	@Path("employees")
	public Map<String, Map<Integer, Object>> getEmployeesStats(
																@QueryParam("site") final String site_pk,
																@QueryParam("department") final Integer dept_pk,
																@QueryParam("date") final String dateStr) {

		final Table<Record6<String, String, Integer, Date, Date, String>> employeesStats = StatisticsHelper
				.selectEmployeesStats(
										dateStr,
										TRAININGS_EMPLOYEES.TREM_EMPL_FK
												.in(Constants.select(	EMPLOYEES.EMPL_PK,
																		this.resourcesHelper.selectEmployees(null, site_pk, dept_pk, null, dateStr))))
				.asTable();

		return this.ctx
				.select(
						employeesStats.field(TRAININGS_EMPLOYEES.TREM_EMPL_FK),
						Constants.arrayAgg(employeesStats.field(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK)).as("cert_pk"),
						Constants.arrayAgg(employeesStats.field("expiry")),
						Constants.arrayAgg(employeesStats.field("opted_out")),
						Constants.arrayAgg(employeesStats.field("validity")))
				.from(employeesStats)
				.groupBy(employeesStats.field(TRAININGS_EMPLOYEES.TREM_EMPL_FK))
				.fetchMap(employeesStats.field(TRAININGS_EMPLOYEES.TREM_EMPL_FK), Constants.getZipMapper(true, "cert_pk", "expiry", "opted_out", "validity"));
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

	public static List<Date> computeDates(final String fromStr, final String toStr, final Integer intervalRaw)
			throws ParseException {
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
