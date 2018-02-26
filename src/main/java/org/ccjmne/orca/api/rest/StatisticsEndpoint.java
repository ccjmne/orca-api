package org.ccjmne.orca.api.rest;

import static org.ccjmne.orca.api.utils.Constants.STATUS_DANGER;
import static org.ccjmne.orca.api.utils.Constants.STATUS_SUCCESS;
import static org.ccjmne.orca.api.utils.Constants.STATUS_WARNING;
import static org.ccjmne.orca.jooq.classes.Tables.CERTIFICATES;
import static org.ccjmne.orca.jooq.classes.Tables.DEPARTMENTS;
import static org.ccjmne.orca.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES_TAGS;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGTYPES_CERTIFICATES;

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
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import org.ccjmne.orca.api.modules.ResourcesUnrestricted;
import org.ccjmne.orca.api.modules.Restrictions;
import org.ccjmne.orca.api.rest.resources.TrainingsStatistics;
import org.ccjmne.orca.api.rest.resources.TrainingsStatistics.TrainingsStatisticsBuilder;
import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.api.utils.ResourcesHelper;
import org.ccjmne.orca.api.utils.RestrictedResourcesAccess;
import org.ccjmne.orca.api.utils.SafeDateFormat;
import org.ccjmne.orca.api.utils.StatisticsHelper;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Table;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;

@Path("statistics")
public class StatisticsEndpoint {

	private static final Integer DEFAULT_INTERVAL = Integer.valueOf(1);

	private final DSLContext ctx;
	private final ResourcesByKeysCommonEndpoint commonResources;
	private final RestrictedResourcesAccess restrictedResourcesAccess;

	// TODO: Should not have any use for these two and should delegate
	// restricted data access mechanics to RestrictedResourcesHelper
	private final ResourcesEndpoint resources;
	private final Restrictions restrictions;

	@Inject
	public StatisticsEndpoint(
								final DSLContext ctx,
								final ResourcesEndpoint resources,
								final ResourcesByKeysCommonEndpoint commonResources,
								final RestrictedResourcesAccess restrictedResourcesAccess,
								final Restrictions restrictions) {
		this.ctx = ctx;
		this.resources = resources;
		this.commonResources = commonResources;
		this.restrictedResourcesAccess = restrictedResourcesAccess;
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

		final Map<Integer, List<Integer>> certs = this.commonResources.listTrainingTypes().entrySet().stream().collect(Collectors
				.toMap(Entry::getKey, e -> Arrays.asList(e.getValue().get(ResourcesUnrestricted.TRAININGTYPE_CERTIFICATES))));
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
	@Deprecated
	public Map<Integer, ? extends Record> getDepartmentStats(
																@PathParam("dept_pk") final Integer dept_pk,
																@QueryParam("date") final String dateStr) {
		return this.ctx.selectQuery(StatisticsHelper
				.selectDepartmentsStats(
										dateStr,
										TRAININGS_EMPLOYEES.TREM_EMPL_FK
												.in(Constants.select(	EMPLOYEES.EMPL_PK,
																		this.restrictedResourcesAccess.selectEmployees(	null, null, dept_pk, null,
																														dateStr))),
										SITES_EMPLOYEES.SIEM_SITE_FK
												.in(Constants.select(SITES.SITE_PK, this.restrictedResourcesAccess.selectSites(null, dept_pk))),
										DEPARTMENTS.DEPT_PK
												.in(Constants.select(DEPARTMENTS.DEPT_PK, this.restrictedResourcesAccess.selectDepartments(dept_pk)))))
				.fetchMap(CERTIFICATES.CERT_PK);
	}

	@GET
	@Path("departments/{dept_pk}/history")
	@Deprecated
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
	@Deprecated
	public Map<Integer, Map<Integer, Object>> getDepartmentsStats(@QueryParam("date") final String dateStr) {
		final Table<? extends Record> departmentsStats = StatisticsHelper
				.selectDepartmentsStats(
										dateStr,
										TRAININGS_EMPLOYEES.TREM_EMPL_FK
												.in(Constants.select(	EMPLOYEES.EMPL_PK,
																		this.restrictedResourcesAccess.selectEmployees(null, null, null, null, dateStr))),
										SITES_EMPLOYEES.SIEM_SITE_FK
												.in(Constants.select(SITES.SITE_PK, this.restrictedResourcesAccess.selectSites(null, null))),
										DEPARTMENTS.DEPT_PK.in(Constants.select(DEPARTMENTS.DEPT_PK, this.restrictedResourcesAccess.selectDepartments(null))))
				.asTable();

		return this.ctx.select(
								departmentsStats.field(DEPARTMENTS.DEPT_PK),
								ResourcesHelper.arrayAgg(departmentsStats.field(CERTIFICATES.CERT_PK)).as("cert_pk"),
								ResourcesHelper.arrayAgg(departmentsStats.field(STATUS_SUCCESS)),
								ResourcesHelper.arrayAgg(departmentsStats.field(STATUS_WARNING)),
								ResourcesHelper.arrayAgg(departmentsStats.field(STATUS_DANGER)),
								ResourcesHelper.arrayAgg(departmentsStats.field("sites_" + STATUS_SUCCESS)),
								ResourcesHelper.arrayAgg(departmentsStats.field("sites_" + STATUS_WARNING)),
								ResourcesHelper.arrayAgg(departmentsStats.field("sites_" + STATUS_DANGER)),
								ResourcesHelper.arrayAgg(departmentsStats.field("score")),
								ResourcesHelper.arrayAgg(departmentsStats.field("validity")))
				.from(departmentsStats)
				.groupBy(departmentsStats.field(DEPARTMENTS.DEPT_PK))
				.fetchMap(
							departmentsStats.field(DEPARTMENTS.DEPT_PK),
							ResourcesHelper.getZipMapper(	"cert_pk", STATUS_SUCCESS, STATUS_WARNING, STATUS_DANGER, "sites_" + STATUS_SUCCESS,
															"sites_" + STATUS_WARNING, "sites_" + STATUS_DANGER, "score", "validity"));
	}

	@GET
	@Path("sites-groups")
	public Object getSitesGroupsStats(@QueryParam("group-by") final Integer tags_pk, @QueryParam("date") final String dateStr, @Context final UriInfo uriInfo) {
		return getSitesGroupsStatsImpl(tags_pk, dateStr, ResourcesHelper.getTagsFromUri(uriInfo));
	}

	/**
	 * Delegates to
	 * {@link StatisticsEndpoint#getSitesGroupsStats(Integer, String, UriInfo)}.<br
	 * />
	 * With this method, the <code>tags_pk</code> argument comes directly from
	 * the query's <strong>path</strong> instead of its parameters.
	 *
	 * @param tags_pk
	 *            The tag to group sites by.
	 */
	@GET
	@Path("sites-groups/{group-by}")
	public Object getSitesGroupsStatsBy(
										@PathParam("group-by") final Integer tags_pk,
										@QueryParam("date") final String dateStr,
										@Context final UriInfo uriInfo) {
		return getSitesGroupsStatsImpl(tags_pk, dateStr, ResourcesHelper.getTagsFromUri(uriInfo));
	}

	@GET
	@Path("sites-groups/{tags_pk}/{sita_value}")
	public Map<Object, Object> getSitesGroupStats(
													@PathParam("tags_pk") final Integer tags_pk,
													@PathParam("sita_value") final String sita_value,
													@QueryParam("date") final String dateStr) {
		return getSitesGroupsStatsImpl(tags_pk, dateStr, Collections.singletonMap(tags_pk, Collections.singletonList(sita_value))).get(sita_value);
	}

	@GET
	@Path("sites-groups/{tags_pk}/{sita_value}/history")
	public Map<Object, Object> getSitesGroupStatsHistory(
															@PathParam("tags_pk") final Integer tags_pk,
															@PathParam("sita_value") final String sita_value,
															@QueryParam("from") final String from,
															@QueryParam("to") final String to,
															@QueryParam("interval") final Integer interval)
			throws ParseException {
		return StatisticsEndpoint.computeDates(from, to, interval).stream()
				.map(date -> Collections.singletonMap(date, getSitesGroupStats(tags_pk, sita_value, date.toString())))
				.reduce(ImmutableMap.<Object, Object> builder(), (res, entry) -> res.putAll(entry), (m1, m2) -> m1.putAll(m2.build())).build();
	}

	private Map<String, Map<Object, Object>> getSitesGroupsStatsImpl(final Integer tags_pk, final String dateStr, final Map<Integer, List<String>> tagFilters) {
		final Table<? extends Record> sitesGroupsStats = StatisticsHelper
				.selectSitesGroupsStats(
										dateStr,
										TRAININGS_EMPLOYEES.TREM_EMPL_FK
												.in(Constants.select(	EMPLOYEES.EMPL_PK,
																		this.restrictedResourcesAccess.selectEmployeesByTags(	null, null, null, dateStr,
																																tagFilters))),
										SITES_EMPLOYEES.SIEM_SITE_FK
												.in(Constants.select(SITES.SITE_PK, this.restrictedResourcesAccess.selectSitesByTags(null, tagFilters))),
										tags_pk)
				.asTable();

		return this.ctx.select(
								sitesGroupsStats.field(SITES_TAGS.SITA_VALUE),
								ResourcesHelper.arrayAgg(sitesGroupsStats.field(CERTIFICATES.CERT_PK)).as("cert_pk"),
								ResourcesHelper.arrayAgg(sitesGroupsStats.field(STATUS_SUCCESS)),
								ResourcesHelper.arrayAgg(sitesGroupsStats.field(STATUS_WARNING)),
								ResourcesHelper.arrayAgg(sitesGroupsStats.field(STATUS_DANGER)),
								ResourcesHelper.arrayAgg(sitesGroupsStats.field("sites_" + STATUS_SUCCESS)),
								ResourcesHelper.arrayAgg(sitesGroupsStats.field("sites_" + STATUS_WARNING)),
								ResourcesHelper.arrayAgg(sitesGroupsStats.field("sites_" + STATUS_DANGER)),
								ResourcesHelper.arrayAgg(sitesGroupsStats.field("score")),
								ResourcesHelper.arrayAgg(sitesGroupsStats.field("validity")))
				.from(sitesGroupsStats)
				.groupBy(sitesGroupsStats.field(SITES_TAGS.SITA_VALUE))
				.fetchMap(
							sitesGroupsStats.field(SITES_TAGS.SITA_VALUE),
							ResourcesHelper.getZipMapper(	"cert_pk", STATUS_SUCCESS, STATUS_WARNING, STATUS_DANGER, "sites_" + STATUS_SUCCESS,
															"sites_" + STATUS_WARNING, "sites_" + STATUS_DANGER, "score", "validity"));
	}

	@GET
	@Path("sites/{site_pk}")
	/**
	 * Could be written in a much simpler fashion using SITM_SITE_FK = SITE_PK,
	 * but that would bypass all the {@link Restrictions} checks in
	 * {@link RestrictedResourcesAccess}
	 */
	public Map<Integer, ? extends Record> getSiteStats(
														@PathParam("site_pk") final String site_pk,
														@QueryParam("date") final String dateStr) {
		return this.ctx.selectQuery(StatisticsHelper
				.selectSitesStats(
									dateStr,
									TRAININGS_EMPLOYEES.TREM_EMPL_FK
											.in(Constants.select(	EMPLOYEES.EMPL_PK,
																	this.restrictedResourcesAccess.selectEmployeesByTags(	null, site_pk, null, dateStr,
																															Collections.emptyMap()))),
									SITES_EMPLOYEES.SIEM_SITE_FK
											.in(Constants.select(	SITES.SITE_PK,
																	this.restrictedResourcesAccess.selectSitesByTags(site_pk, Collections.emptyMap())))))
				.fetchMap(CERTIFICATES.CERT_PK);
	}

	/**
	 * TODO: PostgreSQL's <code>generate_series</code> and only query the DB
	 * once<br />
	 * Code sample:
	 *
	 * <pre>
	 * SELECT generate_series(
	 *     date_trunc('month', '2018-01-23'::date),
	 *     '2018-05-15'::date,
	 *     '1 month'
	 * )::date
	 * </pre>
	 */
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
															@QueryParam("site") final String site_pk,
															@QueryParam("date") final String dateStr,
															@Context final UriInfo uriInfo) {
		final Map<Integer, List<String>> tagFilters = ResourcesHelper.getTagsFromUri(uriInfo);
		final Table<? extends Record> sitesStats = StatisticsHelper
				.selectSitesStats(
									dateStr,
									TRAININGS_EMPLOYEES.TREM_EMPL_FK
											.in(Constants.select(	EMPLOYEES.EMPL_PK,
																	this.restrictedResourcesAccess.selectEmployeesByTags(	null, site_pk, null, dateStr,
																															tagFilters))),
									SITES_EMPLOYEES.SIEM_SITE_FK
											.in(Constants.select(	SITES.SITE_PK,
																	this.restrictedResourcesAccess.selectSitesByTags(site_pk, tagFilters))))
				.asTable();

		return this.ctx.select(
								sitesStats.field(SITES_EMPLOYEES.SIEM_SITE_FK),
								ResourcesHelper.arrayAgg(sitesStats.field(CERTIFICATES.CERT_PK)).as("cert_pk"),
								ResourcesHelper.arrayAgg(sitesStats.field(STATUS_SUCCESS)),
								ResourcesHelper.arrayAgg(sitesStats.field(STATUS_WARNING)),
								ResourcesHelper.arrayAgg(sitesStats.field(STATUS_DANGER)),
								ResourcesHelper.arrayAgg(sitesStats.field("target")),
								ResourcesHelper.arrayAgg(sitesStats.field("validity")))
				.from(sitesStats)
				.groupBy(sitesStats.field(SITES_EMPLOYEES.SIEM_SITE_FK))
				.fetchMap(
							SITES_EMPLOYEES.SIEM_SITE_FK,
							ResourcesHelper.getZipMapper("cert_pk", STATUS_SUCCESS, STATUS_WARNING, STATUS_DANGER, "target", "validity"));
	}

	@GET
	@Path("employees/{empl_pk}")
	public Map<Integer, ? extends Record> getEmployeeStats(
															@PathParam("empl_pk") final String empl_pk,
															@QueryParam("date") final String dateStr) {
		return this.ctx.selectQuery(StatisticsHelper
				.selectEmployeesStats(dateStr, TRAININGS_EMPLOYEES.TREM_EMPL_FK
						.in(Constants.select(	EMPLOYEES.EMPL_PK,
												this.restrictedResourcesAccess.selectEmployeesByTags(empl_pk, null, null, dateStr, Collections.emptyMap())))))
				.fetchMap(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK);
	}

	@GET
	@Path("employees")
	public Map<String, Map<Integer, Object>> getEmployeesStats(
																@QueryParam("employee") final String empl_pk,
																@QueryParam("site") final String site_pk,
																@QueryParam("training") final Integer trng_pk,
																@QueryParam("date") final String dateStr,
																@Context final UriInfo uriInfo) {

		final Table<? extends Record> employeesStats = StatisticsHelper
				.selectEmployeesStats(
										dateStr,
										TRAININGS_EMPLOYEES.TREM_EMPL_FK
												.in(Constants.select(	EMPLOYEES.EMPL_PK,
																		this.restrictedResourcesAccess
																				.selectEmployeesByTags(	empl_pk, site_pk, trng_pk, dateStr,
																										ResourcesHelper.getTagsFromUri(uriInfo)))))
				.asTable();

		return this.ctx
				.select(
						employeesStats.field(TRAININGS_EMPLOYEES.TREM_EMPL_FK),
						ResourcesHelper.arrayAgg(employeesStats.field(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK)).as("cert_pk"),
						ResourcesHelper.arrayAgg(employeesStats.field("expiry")),
						ResourcesHelper.arrayAgg(employeesStats.field("opted_out")),
						ResourcesHelper.arrayAgg(employeesStats.field("validity")))
				.from(employeesStats)
				.groupBy(employeesStats.field(TRAININGS_EMPLOYEES.TREM_EMPL_FK))
				.fetchMap(	employeesStats.field(TRAININGS_EMPLOYEES.TREM_EMPL_FK),
							ResourcesHelper.getZipMapper(true, "cert_pk", "expiry", "opted_out", "validity"));
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
