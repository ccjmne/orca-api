package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.UPDATES;

import java.sql.Date;
import java.text.ParseException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.ccjmne.faomaintenance.api.rest.resources.EmployeeStatistics;
import org.ccjmne.faomaintenance.api.rest.resources.EmployeeStatistics.EmployeeStatisticsBuilder;
import org.ccjmne.faomaintenance.api.rest.resources.SiteStatistics;
import org.ccjmne.faomaintenance.api.utils.SQLDateFormat;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.CertificatesRecord;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.TrainingtypesRecord;
import org.jooq.DSLContext;
import org.jooq.Record;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

@Singleton
@Path("statistics")
public class StatisticsEndpoint {

	private static final int DEFAULT_INTERVAL = 6;
	private final ResourcesEndpoint resources;
	private final ResourcesByKeysEndpoint resourcesByKeys;
	private final SQLDateFormat dateFormat;
	private final DSLContext ctx;

	private final LoadingCache<String, Map.Entry<Date, EmployeeStatistics>> employeeStatisticsCache;
	private final LoadingCache<String, Map.Entry<Date, SiteStatistics>> siteStatisticsCache;
	private final ExecutorService statisticsCalculationThreadPool;
	private Supplier<Map<Integer, TrainingtypesRecord>> trainingTypes;
	private Supplier<Map<Integer, List<Integer>>> certificatesByTrainingTypes;
	private Supplier<Map<Integer, CertificatesRecord>> certificates;

	@Inject
	public StatisticsEndpoint(
								final DSLContext ctx,
								final SQLDateFormat dateFormat,
								final ResourcesEndpoint resources,
								final ResourcesByKeysEndpoint resourcesByKeys) {
		this.ctx = ctx;
		this.dateFormat = dateFormat;
		this.resources = resources;
		this.resourcesByKeys = resourcesByKeys;
		this.statisticsCalculationThreadPool = Executors.newCachedThreadPool();
		this.trainingTypes = Suppliers.memoizeWithExpiration(() -> this.resourcesByKeys.listTrainingTypes(), 1, TimeUnit.DAYS);
		this.certificates = Suppliers.memoizeWithExpiration(() -> this.resourcesByKeys.listCertificates(), 1, TimeUnit.DAYS);
		this.certificatesByTrainingTypes = Suppliers.memoizeWithExpiration(() -> this.resourcesByKeys.listTrainingtypesCertificates(), 1, TimeUnit.DAYS);

		this.employeeStatisticsCache = CacheBuilder
				.newBuilder()
				.refreshAfterWrite(30, TimeUnit.MINUTES)
				.expireAfterAccess(2, TimeUnit.HOURS).<String, Map
				.Entry<Date, EmployeeStatistics>> build(CacheLoader.asyncReloading(CacheLoader.<String, Map.Entry<Date, EmployeeStatistics>> from(empl_pk -> {
					try {
						return StatisticsEndpoint.this.buildLatestEmployeeStats(empl_pk);
					} catch (final Exception e) {
						throw new RuntimeException(e);
					}
				}), this.statisticsCalculationThreadPool));

		this.siteStatisticsCache = CacheBuilder.newBuilder().refreshAfterWrite(1, TimeUnit.HOURS).expireAfterAccess(8, TimeUnit.HOURS).<String, Map
				.Entry<Date, SiteStatistics>> build(CacheLoader.asyncReloading(CacheLoader.<String, Map.Entry<Date, SiteStatistics>> from(site_pk -> {
					try {
						return StatisticsEndpoint.this.calculateLatestSiteStats(site_pk);
					} catch (final Exception e) {
						throw new RuntimeException(e);
					}
				}), this.statisticsCalculationThreadPool));
	}

	public void invalidateSitesStats() {
		this.siteStatisticsCache.invalidateAll();
	}

	public void invalidateSitesStats(final Collection<String> sites) {
		this.siteStatisticsCache.invalidateAll(sites);
	}

	public void invalidateEmployeesStats(final Collection<String> employees) {
		this.employeeStatisticsCache.invalidateAll(employees);
		invalidateSitesStats(this.ctx
				.selectDistinct(SITES_EMPLOYEES.SIEM_SITE_FK)
				.from(SITES_EMPLOYEES)
				.where(
						SITES_EMPLOYEES.SIEM_UPDT_FK.eq(this.ctx.selectFrom(UPDATES).orderBy(UPDATES.UPDT_DATE.desc()).fetchAny(UPDATES.UPDT_PK))
								.and(SITES_EMPLOYEES.SIEM_EMPL_FK.in(employees)))
				.fetch(SITES_EMPLOYEES.SIEM_SITE_FK));

	}

	@GET
	@Path("sites/{site_pk}")
	public Map<Date, SiteStatistics> getSiteStats(
													@PathParam("site_pk") final String site_pk,
													@QueryParam("date") final String dateStr,
													@QueryParam("from") final String fromStr,
													@QueryParam("interval") final Integer interval) throws ParseException {
		if ((dateStr == null) && (fromStr == null)) {
			return new ImmutableMap.Builder<Date, SiteStatistics>().put(this.siteStatisticsCache.getUnchecked(site_pk)).build();
		}

		return calculateSiteStats(site_pk, computeDates(fromStr, dateStr, interval));
	}

	@GET
	@Path("employees/{empl_pk}")
	public Map<Date, EmployeeStatistics> getEmployeeStats(
															@PathParam("empl_pk") final String empl_pk,
															@QueryParam("date") final String dateStr,
															@QueryParam("from") final String fromStr,
															@QueryParam("interval") final Integer interval) throws ParseException {
		if ((dateStr == null) && (fromStr == null)) {
			return new ImmutableMap.Builder<Date, EmployeeStatistics>().put(this.employeeStatisticsCache.getUnchecked(empl_pk)).build();
		}

		return buildEmployeeStats(empl_pk, computeDates(fromStr, dateStr, interval));
	}

	@GET
	@Path("sites")
	public Map<Date, Map<String, SiteStatistics>> getSitesStats(
																@QueryParam("department") final Integer department,
																@QueryParam("employee") final String employee,
																@QueryParam("date") final String dateStr,
																@QueryParam("from") final String fromStr,
																@QueryParam("interval") final Integer interval) throws ParseException {
		final List<String> sites = this.resources.listSites(department, employee, dateStr, false).getValues(SITES.SITE_PK);
		if ((dateStr == null) && (fromStr == null) && (this.siteStatisticsCache.size() >= (sites.size() / 2))) {
			final Builder<String, SiteStatistics> sitesStats = new ImmutableMap.Builder<>();
			for (final String site_pk : sites) {
				sitesStats.put(site_pk, this.siteStatisticsCache.getUnchecked(site_pk).getValue());
			}

			return Collections.singletonMap(new Date(new java.util.Date().getTime()), sitesStats.build());
		}

		final TreeMap<Date, Map<String, SiteStatistics>> res = calculateSitesStats(sites, dateStr, fromStr, interval);
		if (dateStr == null) {
			res.lastEntry().getValue().forEach((site_pk, stats) -> this.siteStatisticsCache.put(site_pk, new SimpleEntry<>(res.lastKey(), stats)));
		}

		return res;
	}

	@GET
	@Path("employees")
	public Map<Date, Map<String, EmployeeStatistics>> getEmployeesStats(
																		@QueryParam("site") final String site_pk,
																		@QueryParam("date") final String dateStr,
																		@QueryParam("from") final String fromStr,
																		@QueryParam("interval") final Integer interval) throws ParseException {
		final List<String> employees = this.resources.listEmployees(site_pk, dateStr, null).getValues(EMPLOYEES.EMPL_PK);
		if ((dateStr == null) && (fromStr == null)) {
			final Builder<String, EmployeeStatistics> employeesStats = new ImmutableMap.Builder<>();
			for (final String empl_pk : employees) {
				employeesStats.put(empl_pk, this.employeeStatisticsCache.getUnchecked(empl_pk).getValue());
			}

			return Collections.singletonMap(new Date(new java.util.Date().getTime()), employeesStats.build());
		}

		final Map<Date, Map<String, EmployeeStatistics>> res = new TreeMap<>();
		final List<Date> dates = computeDates(fromStr, dateStr, interval);
		for (final String empl_pk : employees) {
			buildEmployeeStats(empl_pk, dates).forEach((date, stats) -> res.computeIfAbsent(date, unused -> new HashMap<>()).put(empl_pk, stats));
		}

		return res;
	}

	private TreeMap<Date, Map<String, SiteStatistics>> calculateSitesStats(
																			final List<String> sites,
																			final String dateStr,
																			final String fromStr,
																			final Integer interval) throws ParseException {
		final List<Date> dates = computeDates(fromStr, dateStr, interval);
		final Map<String, Map<Date, EmployeeStatistics>> employeesStats = new HashMap<>();
		final Map<String, Boolean> employees = this.ctx.selectDistinct(SITES_EMPLOYEES.SIEM_EMPL_FK, EMPLOYEES.EMPL_PERMANENT).from(SITES_EMPLOYEES)
				.join(EMPLOYEES).on(SITES_EMPLOYEES.SIEM_EMPL_FK.eq(EMPLOYEES.EMPL_PK)).where(SITES_EMPLOYEES.SIEM_SITE_FK.in(sites))
				.fetchMap(SITES_EMPLOYEES.SIEM_EMPL_FK, EMPLOYEES.EMPL_PERMANENT);
		for (final String empl_pk : employees.keySet()) {
			employeesStats.put(empl_pk, buildEmployeeStats(empl_pk, dates));
		}

		final TreeMap<Date, Map<String, List<String>>> employeesHistory = new TreeMap<>();
		for (final String site_pk : sites) {
			this.ctx.select(SITES_EMPLOYEES.SIEM_EMPL_FK, UPDATES.UPDT_DATE).from(SITES_EMPLOYEES).join(UPDATES)
					.on(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(UPDATES.UPDT_PK)).where(SITES_EMPLOYEES.SIEM_SITE_FK.eq(site_pk))
					.fetchGroups(UPDATES.UPDT_DATE, SITES_EMPLOYEES.SIEM_EMPL_FK)
					.forEach((date, siteEmployees) -> employeesHistory.computeIfAbsent(date, unused -> new HashMap<>()).put(site_pk, siteEmployees));
		}

		final TreeMap<Date, Map<String, SiteStatistics>> res = new TreeMap<>();
		for (final Date date : dates) {
			final Entry<Date, Map<String, List<String>>> mostAccurate = employeesHistory.floorEntry(date);
			if (mostAccurate != null) {
				for (final Entry<String, List<String>> sitesEmployeesHistory : mostAccurate.getValue().entrySet()) {
					final SiteStatistics stats = new SiteStatistics(this.certificates.get());
					sitesEmployeesHistory.getValue().forEach(empl_pk -> stats.register(empl_pk, employees.get(empl_pk), employeesStats.get(empl_pk).get(date)));
					res.computeIfAbsent(date, unused -> new HashMap<>()).put(sitesEmployeesHistory.getKey(), stats);
				}
			} else {
				res.put(date, Collections.emptyMap());
			}
		}

		return res;
	}

	private Map<Date, SiteStatistics> calculateSiteStats(final String site_pk, final List<Date> dates) throws ParseException {
		final Map<String, Map<Date, EmployeeStatistics>> employeesStats = new HashMap<>();
		final Map<String, Boolean> employeesContractTypes = this.ctx.selectDistinct(SITES_EMPLOYEES.SIEM_EMPL_FK, EMPLOYEES.EMPL_PERMANENT)
				.from(SITES_EMPLOYEES)
				.join(EMPLOYEES).on(SITES_EMPLOYEES.SIEM_EMPL_FK.eq(EMPLOYEES.EMPL_PK)).where(SITES_EMPLOYEES.SIEM_SITE_FK.eq(site_pk))
				.fetchMap(SITES_EMPLOYEES.SIEM_EMPL_FK, EMPLOYEES.EMPL_PERMANENT);
		for (final String empl_pk : employeesContractTypes.keySet()) {
			employeesStats.put(empl_pk, buildEmployeeStats(empl_pk, dates));
		}

		final TreeSet<Date> updates = new TreeSet<>(this.ctx.select(UPDATES.UPDT_DATE).from(UPDATES).fetchSet(UPDATES.UPDT_DATE));
		final Map<Date, List<String>> employeesHistory = this.ctx.select(SITES_EMPLOYEES.SIEM_EMPL_FK, UPDATES.UPDT_DATE)
				.from(SITES_EMPLOYEES).join(UPDATES).on(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(UPDATES.UPDT_PK)).where(SITES_EMPLOYEES.SIEM_SITE_FK.eq(site_pk))
				.fetchGroups(UPDATES.UPDT_DATE, SITES_EMPLOYEES.SIEM_EMPL_FK);

		final Map<Date, SiteStatistics> res = new TreeMap<>();
		for (final Date date : dates) {
			final SiteStatistics stats = new SiteStatistics(this.certificates.get());
			final Date mostAccurate = updates.floor(date);
			if (mostAccurate != null) {
				for (final String empl_pk : employeesHistory.getOrDefault(mostAccurate, Collections.emptyList())) {
					stats.register(empl_pk, employeesContractTypes.get(empl_pk), employeesStats.get(empl_pk).get(date));
				}
			}

			res.put(date, stats);
		}

		return res;
	}

	private Map.Entry<Date, SiteStatistics> calculateLatestSiteStats(final String site_pk) throws ParseException {
		final Date currentDate = new Date(new java.util.Date().getTime());
		final Map<String, Boolean> employeesContractTypes = this.ctx.selectDistinct(SITES_EMPLOYEES.SIEM_EMPL_FK, EMPLOYEES.EMPL_PERMANENT)
				.from(SITES_EMPLOYEES)
				.join(EMPLOYEES).on(SITES_EMPLOYEES.SIEM_EMPL_FK.eq(EMPLOYEES.EMPL_PK)).where(SITES_EMPLOYEES.SIEM_SITE_FK.eq(site_pk))
				.fetchMap(SITES_EMPLOYEES.SIEM_EMPL_FK, EMPLOYEES.EMPL_PERMANENT);
		final SiteStatistics stats = new SiteStatistics(this.certificates.get());
		getEmployeesStats(site_pk, null, null, null).values().iterator().next()
				.forEach(
							(empl_pk, empl_stats) -> stats.register(empl_pk, employeesContractTypes.get(empl_pk), empl_stats));
		return new SimpleEntry<>(currentDate, stats);
	}

	private Map<Date, EmployeeStatistics> buildEmployeeStats(
																final String empl_pk,
																final Iterable<Date> dates) throws ParseException {
		final EmployeeStatisticsBuilder builder = EmployeeStatistics.builder(this.trainingTypes.get(), this.certificatesByTrainingTypes.get());
		dates.iterator();
		final Map<Date, EmployeeStatistics> res = new TreeMap<>();

		// TODO: Only retrieve the Training Types that we care about
		final Iterator<Record> trainings = this.resources.listTrainings(empl_pk, Collections.emptyList(), null, null, null).iterator();
		Record training;
		for (final Date nextStop : dates) {
			while (trainings.hasNext() && !nextStop.before((training = trainings.next()).getValue(TRAININGS.TRNG_DATE))) {
				builder.accept(training);
			}

			res.put(nextStop, builder.buildFor(nextStop));
		}

		return res;
	}

	private Map.Entry<Date, EmployeeStatistics> buildLatestEmployeeStats(final String empl_pk) throws ParseException {
		final Date currentDate = new Date(new java.util.Date().getTime());
		final EmployeeStatisticsBuilder builder = EmployeeStatistics.builder(this.trainingTypes.get(), this.certificatesByTrainingTypes.get());
		this.resources.listTrainings(empl_pk, Collections.emptyList(), null, null, currentDate.toString()).forEach(training -> builder.accept(training));
		return new SimpleEntry<>(currentDate, builder.buildFor(currentDate));
	}

	private List<Date> computeDates(final String fromStr, final String toStr, final Integer interval) throws ParseException {
		final Date utmost = (toStr == null) ? new Date(new java.util.Date().getTime()) : this.dateFormat.parseSql(toStr);
		if (fromStr == null) {
			return Collections.singletonList(utmost);
		}

		final Calendar calendar = Calendar.getInstance();
		final List<Date> res = new ArrayList<>();
		calendar.setTime(this.dateFormat.parseSql(fromStr));
		while (calendar.getTime().before(utmost)) {
			res.add(new java.sql.Date(calendar.getTime().getTime()));
			calendar.add(Calendar.MONTH, (interval != null) ? interval.intValue() : DEFAULT_INTERVAL);
		}

		res.add(utmost);
		return res;
	}
}