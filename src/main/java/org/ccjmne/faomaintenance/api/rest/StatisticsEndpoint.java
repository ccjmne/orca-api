package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES_CERTIFICATES_OPTOUT;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGTYPES_CERTIFICATES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.UPDATES;

import java.sql.Date;
import java.text.ParseException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.ccjmne.faomaintenance.api.modules.ResourcesUnrestricted;
import org.ccjmne.faomaintenance.api.modules.Restrictions;
import org.ccjmne.faomaintenance.api.modules.StatisticsCaches;
import org.ccjmne.faomaintenance.api.rest.resources.DepartmentStatistics;
import org.ccjmne.faomaintenance.api.rest.resources.EmployeeStatistics;
import org.ccjmne.faomaintenance.api.rest.resources.EmployeeStatistics.EmployeeStatisticsBuilder;
import org.ccjmne.faomaintenance.api.rest.resources.SiteStatistics;
import org.ccjmne.faomaintenance.api.rest.resources.TrainingsStatistics;
import org.ccjmne.faomaintenance.api.rest.resources.TrainingsStatistics.TrainingsStatisticsBuilder;
import org.ccjmne.faomaintenance.api.utils.Constants;
import org.ccjmne.faomaintenance.api.utils.SafeDateFormat;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.CertificatesRecord;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.TrainingtypesRecord;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record4;
import org.jooq.Record7;
import org.jooq.RecordMapper;
import org.jooq.Result;
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

	/**
	 * Used to list trainings for sites and employees' statistics computing
	 * purposes. Should the current {@link HttpServletRequest} not be able to
	 * directly reach trainings, this would still allow providing statistics for
	 * its accessible sites and employees.<br />
	 * Should <b>not</b> be used within
	 * {@link StatisticsEndpoint#getTrainingsStats(String, String, List)}.
	 */
	private final ResourcesUnrestricted resourcesUnrestricted;

	private final StatisticsCaches statistics;
	private final Restrictions restrictions;

	@Inject
	public StatisticsEndpoint(
								final DSLContext ctx,
								final ResourcesEndpoint resources,
								final ResourcesByKeysCommonEndpoint resourcesByKeys,
								final ResourcesUnrestricted resourcesUnrestricted,
								final StatisticsCaches statisticsCaches,
								final Restrictions restrictions) {
		this.ctx = ctx;
		this.resources = resources;
		this.commonResources = resourcesByKeys;
		this.resourcesUnrestricted = resourcesUnrestricted;
		this.statistics = statisticsCaches;
		this.restrictions = restrictions;
	}

	@GET
	@Path("trainings")
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
	public Map<Date, DepartmentStatistics> getDepartmentStats(
																@PathParam("dept_pk") final Integer dept_pk,
																@QueryParam("date") final String dateStr,
																@QueryParam("from") final String fromStr,
																@QueryParam("interval") final Integer interval)
			throws ParseException {
		if (!this.restrictions.canAccessAllSites() && !this.restrictions.canAccessDepartment(dept_pk)) {
			throw new ForbiddenException();
		}

		final Map<Integer, CertificatesRecord> certificates = this.commonResources.listCertificates();
		final Map<Date, DepartmentStatistics> res = new HashMap<>();
		getSitesStats(dept_pk, null, dateStr, fromStr, interval).forEach((date, sitesStats) -> {
			res.put(date, new DepartmentStatistics(certificates, sitesStats.values()));
		});

		return res;
	}

	@GET
	@Path("sites/{site_pk}")
	public Map<Date, SiteStatistics> getSiteStats(
													@PathParam("site_pk") final String site_pk,
													@QueryParam("date") final String dateStr,
													@QueryParam("from") final String fromStr,
													@QueryParam("interval") final Integer interval)
			throws ParseException {
		if (!this.restrictions.canAccessAllSites() && !this.restrictions.getAccessibleSites().contains(site_pk)) {
			throw new ForbiddenException();
		}

		if ((dateStr == null) && (fromStr == null)) {
			return new ImmutableMap.Builder<Date, SiteStatistics>().put(this.statistics.getSiteStats(site_pk)).build();
		}

		return calculateSiteStats(site_pk, computeDates(fromStr, dateStr, interval));
	}

	@GET
	@Path("sites/v2/{site_pk}")
	public Result<Record7<String, Integer, Integer, Integer, Integer, Integer, String>> getSiteStatsV2(
																										@PathParam("site_pk") final String site_pk,
																										@QueryParam("date") final String dateStr,
																										@QueryParam("from") final String fromStr,
																										@QueryParam("interval") final Integer interval)
			throws ParseException {
		if (!this.restrictions.canAccessAllSites() && !this.restrictions.getAccessibleSites().contains(site_pk)) {
			throw new ForbiddenException();
		}

		return this.ctx.selectQuery(Constants
				.selectSitesStats(
									dateStr,
									TRAININGS_EMPLOYEES.TREM_EMPL_FK.in(this.resources.selectEmployees(site_pk, null, dateStr)),
									SITES_EMPLOYEES.SIEM_SITE_FK.eq(site_pk).and(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(Constants.selectUpdate(dateStr)))))
				.fetch();
	}

	@GET
	@Path("employees/{empl_pk}")
	public Map<Integer, Record4<String, Integer, Date, String>> getEmployeeStatsV2(
																					@PathParam("empl_pk") final String empl_pk,
																					@QueryParam("date") final String dateStr,
																					@QueryParam("from") final String fromStr,
																					@QueryParam("interval") final Integer interval) {
		if (!this.restrictions.canAccessEmployee(empl_pk)) {
			throw new ForbiddenException();
		}

		return this.ctx.selectQuery(Constants
				.selectEmployeesStats(dateStr, TRAININGS_EMPLOYEES.TREM_EMPL_FK.eq(empl_pk)))
				.fetchMap(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK);
	}

	@GET
	@Path("departments")
	public Map<Date, Map<Integer, DepartmentStatistics>> getDepartmentsStats(
																				@QueryParam("date") final String dateStr,
																				@QueryParam("from") final String fromStr,
																				@QueryParam("interval") final Integer interval)
			throws ParseException {
		final Map<Date, Map<Integer, DepartmentStatistics>> res = new HashMap<>();
		if (!this.restrictions.canAccessAllSites()) {
			final Integer dept = this.restrictions.getAccessibleDepartment();
			getDepartmentStats(dept, dateStr, fromStr, interval).forEach((date, departmentStats) -> {
				res.put(date, Collections.singletonMap(dept, departmentStats));
			});

			return res;
		}

		final Map<Integer, CertificatesRecord> certificates = this.commonResources.listCertificates();
		final Map<Integer, List<String>> departmentsSites = this.resources.listSites(null, null, null, false).intoGroups(SITES.SITE_DEPT_FK, SITES.SITE_PK);
		if ((dateStr == null) && (fromStr == null)) {
			final Map<Integer, DepartmentStatistics> entry = new HashMap<>();
			departmentsSites.entrySet().stream().forEach(deptSites -> {
				final DepartmentStatistics deptStats = new DepartmentStatistics(certificates, deptSites.getValue().stream()
						.map(site -> this.statistics.getSiteStats(site).getValue()).collect(Collectors.toList()));
				entry.put(deptSites.getKey(), deptStats);
			});

			return Collections.singletonMap(new Date(new java.util.Date().getTime()), entry);
		}

		getSitesStats(null, null, dateStr, fromStr, interval).forEach((date, sitesStats) -> {
			final Map<Integer, DepartmentStatistics> entry = new HashMap<>();
			departmentsSites.entrySet().stream().forEach(deptSites -> {
				final DepartmentStatistics deptStats = new DepartmentStatistics(certificates, deptSites.getValue().stream()
						.map(site -> sitesStats.get(site)).collect(Collectors.toList()));
				entry.put(deptSites.getKey(), deptStats);
			});

			res.put(date, entry);
		});

		return res;
	}

	@GET
	@Path("sites")
	public Map<Date, Map<String, SiteStatistics>> getSitesStats(
																@QueryParam("department") final Integer dept_pk,
																@QueryParam("employee") final String empl_pk,
																@QueryParam("date") final String dateStr,
																@QueryParam("from") final String fromStr,
																@QueryParam("interval") final Integer interval)
			throws ParseException {
		if ((empl_pk != null) && !this.restrictions.canAccessEmployee(empl_pk)) {
			throw new ForbiddenException();
		}

		if ((dept_pk != null) && !this.restrictions.canAccessDepartment(dept_pk)) {
			throw new ForbiddenException();
		}

		final List<String> sites = this.resources.listSites(dept_pk, empl_pk, dateStr, false).getValues(SITES.SITE_PK);
		if ((dateStr == null) && (fromStr == null)) {
			final Builder<String, SiteStatistics> sitesStats = new ImmutableMap.Builder<>();
			for (final String site_pk : sites) {
				sitesStats.put(site_pk, this.statistics.getSiteStats(site_pk).getValue());
			}

			return Collections.singletonMap(new Date(new java.util.Date().getTime()), sitesStats.build());
		}

		final TreeMap<Date, Map<String, SiteStatistics>> res = calculateSitesStats(sites, dateStr, fromStr, interval);
		if (dateStr == null) {
			res.lastEntry().getValue().forEach((site_pk, stats) -> this.statistics.putSitesStats(site_pk, res.lastKey(), stats));
		}

		return res;
	}

	@GET
	@Path("employees")
	public Map<String, Map<Integer, Object>> getEmployeesStatsV2(
																	@QueryParam("site") final String site_pk,
																	@QueryParam("department") final Integer dept_pk,
																	@QueryParam("date") final String dateStr)
			throws ParseException {

		final Table<Record4<String, Integer, Date, String>> employeesStats = Constants
				.selectEmployeesStats(
										dateStr,
										TRAININGS_EMPLOYEES.TREM_EMPL_FK.in(this.resources.selectEmployees(site_pk, dept_pk, dateStr)))
				.asTable();

		return this.ctx
				.select(
						employeesStats.field(TRAININGS_EMPLOYEES.TREM_EMPL_FK),
						DSL.arrayAgg(employeesStats.field(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK)).as("cert_pk"),
						DSL.arrayAgg(employeesStats.field("expiry")).as("expiry"),
						DSL.arrayAgg(employeesStats.field("validity")).as("validity"))
				.from(employeesStats)
				.groupBy(employeesStats.field(TRAININGS_EMPLOYEES.TREM_EMPL_FK))
				.fetchMap(employeesStats.field(TRAININGS_EMPLOYEES.TREM_EMPL_FK), new RecordMapper<Record, Map<Integer, Object>>() {

					@Override
					public Map<Integer, Object> map(final Record record) {
						final Map<Integer, Object> res = new HashMap<>();
						final Integer[] certificates = (Integer[]) record.get("cert_pk");
						for (int i = 0; i < certificates.length; i++) {
							final Integer cert_pk = certificates[i];
							res.put(cert_pk, ImmutableMap
									.<String, Object> of(
															"expiry",
															((Object[]) record.get("expiry"))[i],
															"validity",
															((Object[]) record.get("validity"))[i]));
						}

						return res;
					}
				});
	}

	/**
	 * Specifically allowed to directly use the {@link DSLContext} with
	 * virtually no restriction.<br />
	 * Reasons:
	 * <ol>
	 * <li><code>private</code> method <b>shielded</b> by the caller.</li>
	 * <li>Can access employees that are not in the accessible sites anymore.
	 * </li>
	 * </ol>
	 */
	private TreeMap<Date, Map<String, SiteStatistics>> calculateSitesStats(
																			final List<String> sites,
																			final String dateStr,
																			final String fromStr,
																			final Integer interval)
			throws ParseException {
		final Map<Integer, TrainingtypesRecord> trainingTypes = this.commonResources.listTrainingTypes();
		final Map<Integer, List<Integer>> trainingtypesCertificates = this.commonResources.listTrainingtypesCertificates();
		final List<Date> dates = computeDates(fromStr, dateStr, interval);
		final Map<String, Map<Date, EmployeeStatistics>> employeesStats = new HashMap<>();
		for (final String empl_pk : this.ctx.selectDistinct(SITES_EMPLOYEES.SIEM_EMPL_FK)
				.from(SITES_EMPLOYEES).where(SITES_EMPLOYEES.SIEM_SITE_FK.in(sites))
				.fetch(SITES_EMPLOYEES.SIEM_EMPL_FK)) {
			employeesStats.put(empl_pk, buildEmployeeStats(empl_pk, dates, trainingTypes, trainingtypesCertificates));
		}

		final TreeMap<Date, Map<String, Result<? extends Record>>> employeesHistory = new TreeMap<>();
		for (final String site_pk : sites) {
			this.resources.getSiteEmployeesHistory(site_pk)
					.forEach((date, employees) -> {
						employeesHistory.computeIfAbsent(date, unused -> new HashMap<>()).put(site_pk, employees);
					});
		}

		final Map<Integer, CertificatesRecord> certificates = this.commonResources.listCertificates();
		final TreeMap<Date, Map<String, SiteStatistics>> res = new TreeMap<>();
		for (final Date date : dates) {
			final Entry<Date, Map<String, Result<? extends Record>>> mostAccurate = employeesHistory.floorEntry(date);
			if (mostAccurate != null) {
				for (final Entry<String, Result<? extends Record>> sitesEmployeesHistory : mostAccurate.getValue().entrySet()) {
					final SiteStatistics stats = new SiteStatistics(certificates);
					for (final Record empl : sitesEmployeesHistory.getValue()) {
						stats.register(
										empl.getValue(EMPLOYEES.EMPL_PERMANENT),
										employeesStats.get(empl.getValue(EMPLOYEES.EMPL_PK)).get(date));
					}

					res.computeIfAbsent(date, unused -> new HashMap<>()).put(sitesEmployeesHistory.getKey(), stats);
				}
			} else {
				res.put(date, Collections.EMPTY_MAP);
			}
		}

		return res;
	}

	/**
	 * Specifically allowed to directly use the {@link DSLContext} with
	 * virtually no restriction.<br />
	 * Reasons:
	 * <ol>
	 * <li><code>private</code> method <b>shielded</b> by the caller.</li>
	 * <li>Can access employees that are not in the accessible sites anymore.
	 * </li>
	 * </ol>
	 */
	private Map<Date, SiteStatistics> calculateSiteStats(final String site_pk, final List<Date> dates) {
		final Map<Integer, TrainingtypesRecord> trainingTypes = this.commonResources.listTrainingTypes();
		final Map<Integer, List<Integer>> trainingtypesCertificates = this.commonResources.listTrainingtypesCertificates();
		final Map<String, Map<Date, EmployeeStatistics>> employeesStats = new HashMap<>();
		for (final String empl_pk : this.ctx.selectDistinct(SITES_EMPLOYEES.SIEM_EMPL_FK)
				.from(SITES_EMPLOYEES).where(SITES_EMPLOYEES.SIEM_SITE_FK.eq(site_pk))
				.fetch(SITES_EMPLOYEES.SIEM_EMPL_FK)) {
			employeesStats.put(empl_pk, buildEmployeeStats(empl_pk, dates, trainingTypes, trainingtypesCertificates));
		}

		final TreeSet<Date> updates = new TreeSet<>(this.resources.listUpdates().getValues(UPDATES.UPDT_DATE));
		final Map<Date, Result<Record4<String, Boolean, String, Date>>> employeesHistory = this.resources.getSiteEmployeesHistory(site_pk);

		final Map<Integer, CertificatesRecord> certificates = this.commonResources.listCertificates();
		final Map<Date, SiteStatistics> res = new TreeMap<>();
		for (final Date date : dates) {
			final SiteStatistics stats = new SiteStatistics(certificates);
			final Date mostAccurate = updates.floor(date);
			if ((mostAccurate != null) && employeesHistory.containsKey(mostAccurate)) {
				for (final Record empl : employeesHistory.get(mostAccurate)) {
					stats.register(
									empl.getValue(EMPLOYEES.EMPL_PERMANENT),
									employeesStats.get(empl.getValue(EMPLOYEES.EMPL_PK)).get(date));
				}
			}

			res.put(date, stats);
		}

		return res;
	}

	private Map<Date, EmployeeStatistics> buildEmployeeStats(
																final String empl_pk,
																final Iterable<Date> dates,
																final Map<Integer, TrainingtypesRecord> trainingTypes,
																final Map<Integer, List<Integer>> certificatesByTrainingTypes) {
		final EmployeeStatisticsBuilder builder = EmployeeStatistics
				.builder(
							certificatesByTrainingTypes,
							this.resourcesUnrestricted.listCertificatesVoiding(empl_pk)
									.intoMap(EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_CERT_FK, EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_DATE));
		final Map<Date, EmployeeStatistics> res = new TreeMap<>();
		final Iterator<Record> trainings = this.resourcesUnrestricted.listTrainings(empl_pk).iterator();
		Record training = trainings.hasNext() ? trainings.next() : null;
		for (final Date nextStop : dates) {
			while ((training != null) && !nextStop.before(training.getValue(TRAININGS.TRNG_DATE))) {
				builder.accept(training);
				training = trainings.hasNext() ? trainings.next() : null;
			}

			res.put(nextStop, builder.buildFor(nextStop));
		}

		return res;
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