package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES_CERTIFICATES_OPTOUT;
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
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.ccjmne.faomaintenance.api.modules.ResourcesUnrestricted;
import org.ccjmne.faomaintenance.api.modules.Restrictions;
import org.ccjmne.faomaintenance.api.modules.StatisticsCaches;
import org.ccjmne.faomaintenance.api.rest.resources.EmployeeStatistics;
import org.ccjmne.faomaintenance.api.rest.resources.EmployeeStatistics.EmployeeStatisticsBuilder;
import org.ccjmne.faomaintenance.api.rest.resources.SiteStatistics;
import org.ccjmne.faomaintenance.api.rest.resources.TrainingsStatistics;
import org.ccjmne.faomaintenance.api.rest.resources.TrainingsStatistics.TrainingsStatisticsBuilder;
import org.ccjmne.faomaintenance.api.utils.Constants;
import org.ccjmne.faomaintenance.api.utils.ForbiddenException;
import org.ccjmne.faomaintenance.api.utils.SafeDateFormat;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.CertificatesRecord;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.TrainingtypesRecord;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;

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
								final ResourcesByKeysEndpoint resourcesByKeys,
								final ResourcesUnrestricted resourcesUnrestricted,
								final StatisticsCaches statisticsCaches,
								final Restrictions restrictions) {
		this.ctx = ctx;
		this.resources = resources;
		this.resourcesByKeys = resourcesByKeys;
		this.resourcesUnrestricted = resourcesUnrestricted;
		this.statistics = statisticsCaches;
		this.restrictions = restrictions;
	}

	@GET
	@Path("trainings")
	public Map<Integer, Iterable<TrainingsStatistics>> getTrainingsStats(
																			@QueryParam("from") final String fromStr,
																			@QueryParam("to") final String toStr,
																			@QueryParam("interval") final List<Integer> intervals) throws ParseException {
		if (!this.restrictions.canAccessTrainings()) {
			throw new ForbiddenException();
		}

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

	@GET
	@Path("sites/{site_pk}")
	public Map<Date, SiteStatistics> getSiteStats(
													@PathParam("site_pk") final String site_pk,
													@QueryParam("date") final String dateStr,
													@QueryParam("from") final String fromStr,
													@QueryParam("interval") final Integer interval) throws ParseException {
		if (!this.restrictions.canAccessAllSites() && !this.restrictions.getAccessibleSites().contains(site_pk)) {
			throw new ForbiddenException();
		}

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
		if (!this.restrictions.canAccessEmployee(empl_pk)) {
			throw new ForbiddenException();
		}

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
																@QueryParam("department") final Integer dept_pk,
																@QueryParam("employee") final String empl_pk,
																@QueryParam("date") final String dateStr,
																@QueryParam("from") final String fromStr,
																@QueryParam("interval") final Integer interval) throws ParseException {
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
	public Map<Date, Map<String, EmployeeStatistics>> getEmployeesStats(
																		@QueryParam("site") final String site_pk,
																		@QueryParam("date") final String dateStr,
																		@QueryParam("from") final String fromStr,
																		@QueryParam("interval") final Integer interval) throws ParseException {
		if ((site_pk != null) && !this.restrictions.canAccessAllSites() && !this.restrictions.getAccessibleSites().contains(site_pk)) {
			throw new ForbiddenException();
		}

		final List<String> employees = this.resources.listEmployees(site_pk, dateStr, null).getValues(EMPLOYEES.EMPL_PK);
		if ((dateStr == null) && (fromStr == null)) {
			final Builder<String, EmployeeStatistics> employeesStats = new ImmutableMap.Builder<>();
			for (final String empl_pk : employees) {
				employeesStats.put(empl_pk, this.statistics.getEmployeeStats(empl_pk).getValue());
			}

			return Collections.singletonMap(new Date(new java.util.Date().getTime()), employeesStats.build());
		}

		// TODO: Bulk employees stats computing
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

	/**
	 * Specifically allowed to directly use the {@link DSLContext} with
	 * virtually no restriction.<br />
	 * Reason: <code>private</code> method <b>shielded</b> by the caller.
	 */
	private TreeMap<Date, Map<String, SiteStatistics>> calculateSitesStats(
																			final List<String> sites,
																			final String dateStr,
																			final String fromStr,
																			final Integer interval) throws ParseException {
		final Map<Integer, TrainingtypesRecord> trainingTypes = this.resourcesByKeys.listTrainingTypes();
		final Map<Integer, List<Integer>> trainingtypesCertificates = this.resourcesByKeys.listTrainingtypesCertificates();
		final List<Date> dates = computeDates(fromStr, dateStr, interval);
		final Map<String, Map<Date, EmployeeStatistics>> employeesStats = new HashMap<>();
		for (final String empl_pk : this.ctx.selectDistinct(SITES_EMPLOYEES.SIEM_EMPL_FK)
				.from(SITES_EMPLOYEES).where(SITES_EMPLOYEES.SIEM_SITE_FK.in(sites))
				.fetch(SITES_EMPLOYEES.SIEM_EMPL_FK)) {
			employeesStats.put(empl_pk, buildEmployeeStats(empl_pk, dates, trainingTypes, trainingtypesCertificates));
		}

		final TreeMap<Date, Map<String, Result<Record>>> employeesHistory = new TreeMap<>();
		for (final String site_pk : sites) {
			this.resources.getSiteEmployeesHistory(site_pk)
					.forEach((date, employees) -> {
						employeesHistory.computeIfAbsent(date, unused -> new HashMap<>()).put(site_pk, employees);
					});
		}

		final Map<Integer, CertificatesRecord> certificates = this.resourcesByKeys.listCertificates();
		final TreeMap<Date, Map<String, SiteStatistics>> res = new TreeMap<>();
		for (final Date date : dates) {
			final Entry<Date, Map<String, Result<Record>>> mostAccurate = employeesHistory.floorEntry(date);
			if (mostAccurate != null) {
				for (final Entry<String, Result<Record>> sitesEmployeesHistory : mostAccurate.getValue().entrySet()) {
					final SiteStatistics stats = new SiteStatistics(certificates);
					for (final Record empl : sitesEmployeesHistory.getValue()) {
						stats.register(
										empl.getValue(EMPLOYEES.EMPL_PK),
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
	 * Reason: <code>private</code> method <b>shielded</b> by the caller.
	 */
	private Map<Date, SiteStatistics> calculateSiteStats(final String site_pk, final List<Date> dates) {
		final Map<Integer, TrainingtypesRecord> trainingTypes = this.resourcesByKeys.listTrainingTypes();
		final Map<Integer, List<Integer>> trainingtypesCertificates = this.resourcesByKeys.listTrainingtypesCertificates();
		final Map<String, Map<Date, EmployeeStatistics>> employeesStats = new HashMap<>();
		for (final String empl_pk : this.ctx.selectDistinct(SITES_EMPLOYEES.SIEM_EMPL_FK)
				.from(SITES_EMPLOYEES).where(SITES_EMPLOYEES.SIEM_SITE_FK.eq(site_pk))
				.fetch(SITES_EMPLOYEES.SIEM_EMPL_FK)) {
			employeesStats.put(empl_pk, buildEmployeeStats(empl_pk, dates, trainingTypes, trainingtypesCertificates));
		}

		final TreeSet<Date> updates = new TreeSet<>(this.resources.listUpdates().getValues(UPDATES.UPDT_DATE));
		final Map<Date, Result<Record>> employeesHistory = this.resources.getSiteEmployeesHistory(site_pk);

		final Map<Integer, CertificatesRecord> certificates = this.resourcesByKeys.listCertificates();
		final Map<Date, SiteStatistics> res = new TreeMap<>();
		for (final Date date : dates) {
			final SiteStatistics stats = new SiteStatistics(certificates);
			final Date mostAccurate = updates.floor(date);
			if ((mostAccurate != null) && employeesHistory.containsKey(mostAccurate)) {
				for (final Record empl : employeesHistory.get(mostAccurate)) {
					stats.register(
									empl.getValue(EMPLOYEES.EMPL_PK),
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
							this.resources.getEmployeeVoiding(empl_pk)
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