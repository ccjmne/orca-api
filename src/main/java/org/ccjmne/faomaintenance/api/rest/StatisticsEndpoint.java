package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS;
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
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.ccjmne.faomaintenance.api.rest.resources.EmployeeStatistics;
import org.ccjmne.faomaintenance.api.rest.resources.EmployeeStatistics.EmployeeStatisticsBuilder;
import org.ccjmne.faomaintenance.api.rest.resources.SiteStatistics;
import org.ccjmne.faomaintenance.api.rest.resources.TrainingsStatistics;
import org.ccjmne.faomaintenance.api.rest.resources.TrainingsStatistics.TrainingsStatisticsBuilder;
import org.ccjmne.faomaintenance.api.utils.Constants;
import org.ccjmne.faomaintenance.api.utils.SafeDateFormat;
import org.ccjmne.faomaintenance.api.utils.StatisticsCaches;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.CertificatesRecord;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.TrainingtypesRecord;
import org.jooq.DSLContext;
import org.jooq.Record;

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
	private final ResourcesByKeysEndpoint resourcesByKeys;

	private final StatisticsCaches statistics;

	@Inject
	public StatisticsEndpoint(
								final DSLContext ctx,
								final ResourcesEndpoint resources,
								final ResourcesByKeysEndpoint resourcesByKeys,
								final StatisticsCaches statisticsCaches) {
		this.ctx = ctx;
		this.resources = resources;
		this.resourcesByKeys = resourcesByKeys;
		this.statistics = statisticsCaches;
	}

	@GET
	@Path("trainings")
	public Map<Integer, Iterable<TrainingsStatistics>> getTrainingsStats(
																			@QueryParam("from") final String fromStr,
																			@QueryParam("to") final String toStr,
																			@QueryParam("interval") final List<Integer> intervals) throws ParseException {
		final List<Record> trainings = this.resources.listTrainings(null, Collections.EMPTY_LIST, null, null, null).stream()
				.filter(trng -> Constants.TRNG_OUTCOME_COMPLETED.equals(trng.getValue(TRAININGS.TRNG_OUTCOME))).collect(Collectors.toList());
		final List<Record> trainingsByExpiry = this.resources
				.listTrainings(null, Collections.EMPTY_LIST, null, null, null).sortAsc(Constants.TRAINING_EXPIRY).stream()
				.filter(trng -> Constants.TRNG_OUTCOME_COMPLETED.equals(trng.getValue(TRAININGS.TRNG_OUTCOME))).collect(Collectors.toList());

		final Map<Integer, List<Integer>> certs = this.resourcesByKeys.listTrainingtypesCertificates();
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

	@GET
	@Path("sites/{site_pk}")
	public Map<Date, SiteStatistics> getSiteStats(
													@PathParam("site_pk") final String site_pk,
													@QueryParam("date") final String dateStr,
													@QueryParam("from") final String fromStr,
													@QueryParam("interval") final Integer interval) throws ParseException {
		if ((dateStr == null) && (fromStr == null)) {
			return new ImmutableMap.Builder<Date, SiteStatistics>().put(this.statistics.getSiteStats(site_pk)).build();
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
			return new ImmutableMap.Builder<Date, EmployeeStatistics>().put(this.statistics.getEmployeeStats(empl_pk)).build();
		}

		return buildEmployeeStats(
									empl_pk,
									computeDates(fromStr, dateStr, interval),
									this.resourcesByKeys.listTrainingTypes(),
									this.resourcesByKeys.listTrainingtypesCertificates());
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
	public Map<Date, Map<String, EmployeeStatistics>> getEmployeesStats(
																		@QueryParam("site") final String site_pk,
																		@QueryParam("date") final String dateStr,
																		@QueryParam("from") final String fromStr,
																		@QueryParam("interval") final Integer interval) throws ParseException {
		final List<String> employees = this.resources.listEmployees(site_pk, dateStr, null).getValues(EMPLOYEES.EMPL_PK);
		// TODO: Bulk employees stats computing when cache isn't reasonably full
		// + store in cache. Just like SitesStatistics.
		if ((dateStr == null) && (fromStr == null)) {
			final Builder<String, EmployeeStatistics> employeesStats = new ImmutableMap.Builder<>();
			for (final String empl_pk : employees) {
				employeesStats.put(empl_pk, this.statistics.getEmployeeStats(empl_pk).getValue());
			}

			return Collections.singletonMap(new Date(new java.util.Date().getTime()), employeesStats.build());
		}

		final Map<Integer, TrainingtypesRecord> trainingTypes = this.resourcesByKeys.listTrainingTypes();
		final Map<Integer, List<Integer>> trainingtypesCertificates = this.resourcesByKeys.listTrainingtypesCertificates();
		final List<Date> dates = computeDates(fromStr, dateStr, interval);
		final Map<Date, Map<String, EmployeeStatistics>> res = new TreeMap<>();
		for (final String empl_pk : employees) {
			buildEmployeeStats(empl_pk, dates, trainingTypes, trainingtypesCertificates)
					.forEach((date, stats) -> res.computeIfAbsent(date, unused -> new HashMap<>()).put(empl_pk, stats));
		}

		return res;
	}

	// TODO: use in StatisticsCaches as an init pre-fill
	public TreeMap<Date, Map<String, SiteStatistics>> calculateSitesStats(
																			final List<String> sites,
																			final String dateStr,
																			final String fromStr,
																			final Integer interval) throws ParseException {
		final Map<Integer, TrainingtypesRecord> trainingTypes = this.resourcesByKeys.listTrainingTypes();
		final Map<Integer, List<Integer>> trainingtypesCertificates = this.resourcesByKeys.listTrainingtypesCertificates();
		final List<Date> dates = computeDates(fromStr, dateStr, interval);
		final Map<String, Map<Date, EmployeeStatistics>> employeesStats = new HashMap<>();
		final Map<String, Boolean> employeesStatus = this.statistics.allEmployeesEverForSites(sites);
		for (final String empl_pk : employeesStatus.keySet()) {
			employeesStats.put(empl_pk, buildEmployeeStats(empl_pk, dates, trainingTypes, trainingtypesCertificates));
		}

		final TreeMap<Date, Map<String, List<String>>> employeesHistory = new TreeMap<>();
		for (final String site_pk : sites) {
			employeesHistoryForSite(site_pk)
					.forEach((date, siteEmployees) -> employeesHistory.computeIfAbsent(date, unused -> new HashMap<>()).put(site_pk, siteEmployees));
		}

		final Map<Integer, CertificatesRecord> certificates = this.resourcesByKeys.listCertificates();
		final TreeMap<Date, Map<String, SiteStatistics>> res = new TreeMap<>();
		for (final Date date : dates) {
			final Entry<Date, Map<String, List<String>>> mostAccurate = employeesHistory.floorEntry(date);
			if (mostAccurate != null) {
				for (final Entry<String, List<String>> sitesEmployeesHistory : mostAccurate.getValue().entrySet()) {
					final SiteStatistics stats = new SiteStatistics(certificates);
					sitesEmployeesHistory.getValue()
							.forEach(empl_pk -> stats.register(empl_pk, employeesStatus.get(empl_pk), employeesStats.get(empl_pk).get(date)));
					res.computeIfAbsent(date, unused -> new HashMap<>()).put(sitesEmployeesHistory.getKey(), stats);
				}
			} else {
				res.put(date, Collections.EMPTY_MAP);
			}
		}

		return res;
	}

	private Map<Date, SiteStatistics> calculateSiteStats(final String site_pk, final List<Date> dates) {
		final Map<Integer, TrainingtypesRecord> trainingTypes = this.resourcesByKeys.listTrainingTypes();
		final Map<Integer, List<Integer>> trainingtypesCertificates = this.resourcesByKeys.listTrainingtypesCertificates();
		final Map<String, Map<Date, EmployeeStatistics>> employeesStats = new HashMap<>();
		final Map<String, Boolean> employeesContractTypes = this.statistics.allEmployeesEverForSites(Collections.singletonList(site_pk));
		for (final String empl_pk : employeesContractTypes.keySet()) {
			employeesStats.put(empl_pk, buildEmployeeStats(empl_pk, dates, trainingTypes, trainingtypesCertificates));
		}

		final TreeSet<Date> updates = new TreeSet<>(this.resources.listUpdates().getValues(UPDATES.UPDT_DATE));
		final Map<Date, List<String>> employeesHistory = employeesHistoryForSite(site_pk);

		final Map<Integer, CertificatesRecord> certificates = this.resourcesByKeys.listCertificates();
		final Map<Date, SiteStatistics> res = new TreeMap<>();
		for (final Date date : dates) {
			final SiteStatistics stats = new SiteStatistics(certificates);
			final Date mostAccurate = updates.floor(date);
			if (mostAccurate != null) {
				for (final String empl_pk : employeesHistory.getOrDefault(mostAccurate, Collections.EMPTY_LIST)) {
					stats.register(empl_pk, employeesContractTypes.get(empl_pk), employeesStats.get(empl_pk).get(date));
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
		final EmployeeStatisticsBuilder builder = EmployeeStatistics.builder(certificatesByTrainingTypes, this.statistics.buildCertificatesVoiding(empl_pk));
		final Map<Date, EmployeeStatistics> res = new TreeMap<>();
		final Iterator<Record> trainings = this.resources.listTrainingsUnrestricted(empl_pk, Collections.EMPTY_LIST, null, null, null).iterator();
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

	/**
	 * Allowed to directly use the {@link DSLContext} with no access
	 * restriction.<br />
	 * Reason: <b>statistics building</b>.
	 */
	private Map<Date, List<String>> employeesHistoryForSite(final String site_pk) {
		return this.ctx.select(SITES_EMPLOYEES.SIEM_EMPL_FK, UPDATES.UPDT_DATE).from(SITES_EMPLOYEES)
				.join(UPDATES).on(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(UPDATES.UPDT_PK))
				.where(SITES_EMPLOYEES.SIEM_SITE_FK.eq(site_pk))
				.fetchGroups(UPDATES.UPDT_DATE, SITES_EMPLOYEES.SIEM_EMPL_FK);
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