package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.CERTIFICATES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.UPDATES;

import java.sql.Date;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.ccjmne.faomaintenance.api.rest.resources.EmployeeStatistics;
import org.ccjmne.faomaintenance.api.rest.resources.EmployeeStatistics.EmployeeStatisticsBuilder;
import org.ccjmne.faomaintenance.api.rest.resources.SiteStatistics;
import org.ccjmne.faomaintenance.api.utils.SQLDateFormat;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.TrainingtypesRecord;
import org.jooq.DSLContext;
import org.jooq.Record;

@Path("statistics")
public class StatisticsEndpoint {

	private static final int DEFAULT_INTERVAL = 6;
	private final ResourcesEndpoint resources;
	private final ResourcesByKeysEndpoint resourcesByKeys;
	private final SQLDateFormat dateFormat;
	private final DSLContext ctx;

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
	}

	@GET
	@Path("sites")
	public Map<Date, Map<String, SiteStatistics>> statsSites(
																@QueryParam("department") final Integer department,
																@QueryParam("date") final String dateStr,
																@QueryParam("from") final String fromStr,
																@QueryParam("interval") final Integer interval) throws ParseException {
		final Date asOf = (dateStr == null) ? new Date(new java.util.Date().getTime()) : this.dateFormat.parseSql(dateStr);
		final List<Date> dates = (fromStr == null) ? Collections.singletonList(asOf) : getDates(
																								(interval != null) ? interval.intValue() : DEFAULT_INTERVAL,
																								this.dateFormat.parseSql(fromStr),
																								asOf);
		final Map<Integer, TrainingtypesRecord> trainingTypes = this.resourcesByKeys.listTrainingTypes();
		final Map<Integer, List<Integer>> certificatesByTrainingType = this.resourcesByKeys.listTrainingtypesCertificates();
		final List<Integer> certificates = this.resources.listCertificates().getValues(CERTIFICATES.CERT_PK);
		final Map<String, Map<Date, EmployeeStatistics>> employeesStatistics = new HashMap<>();
		final List<String> sites = this.resources.listSites(department, dateStr).getValues(SITES.SITE_PK);
		final Map<String, Boolean> employees = this.ctx.selectDistinct(SITES_EMPLOYEES.SIEM_EMPL_FK, EMPLOYEES.EMPL_PERMANENT).from(SITES_EMPLOYEES)
				.join(EMPLOYEES).on(SITES_EMPLOYEES.SIEM_EMPL_FK.eq(EMPLOYEES.EMPL_PK)).where(SITES_EMPLOYEES.SIEM_SITE_FK.in(sites))
				.fetchMap(SITES_EMPLOYEES.SIEM_EMPL_FK, EMPLOYEES.EMPL_PERMANENT);
		for (final String empl_pk : employees.keySet()) {
			employeesStatistics.put(
									empl_pk,
									getEmployeeStatsForDates(empl_pk, dateStr, dates, trainingTypes, certificatesByTrainingType));
		}

		final TreeMap<Date, Map<String, List<String>>> employeesHistory = new TreeMap<>();
		for (final String site_pk : sites) {
			this.ctx.select(SITES_EMPLOYEES.SIEM_EMPL_FK, UPDATES.UPDT_DATE).from(SITES_EMPLOYEES).join(UPDATES)
					.on(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(UPDATES.UPDT_PK)).where(SITES_EMPLOYEES.SIEM_SITE_FK.eq(site_pk))
					.fetchGroups(UPDATES.UPDT_DATE, SITES_EMPLOYEES.SIEM_EMPL_FK)
					.forEach((date, siteEmployees) -> employeesHistory.computeIfAbsent(date, unused -> new HashMap<>()).put(site_pk, siteEmployees));
		}

		final Map<Date, Map<String, SiteStatistics>> res = new TreeMap<>();
		for (final Date date : dates) {
			final Entry<Date, Map<String, List<String>>> mostAccurate = employeesHistory.floorEntry(date);
			if (mostAccurate != null) {
				for (final Entry<String, List<String>> siteHistory : mostAccurate.getValue().entrySet()) {
					final SiteStatistics stats = new SiteStatistics(certificates);
					siteHistory.getValue().forEach(rNumber -> stats.register(rNumber, employees.get(rNumber), employeesStatistics.get(rNumber).get(date)));
					res.computeIfAbsent(date, unused -> new HashMap<>()).put(siteHistory.getKey(), stats);
				}
			} else {
				res.put(date, Collections.EMPTY_MAP);
			}
		}

		return res;
	}

	@GET
	@Path("sites/{site_pk}")
	public Map<Date, SiteStatistics> statsSite(
												@PathParam("site_pk") final String site_pk,
												@QueryParam("date") final String dateStr,
												@QueryParam("from") final String fromStr,
												@QueryParam("interval") final Integer interval) throws ParseException {
		final Date asOf = (dateStr == null) ? new Date(new java.util.Date().getTime()) : this.dateFormat.parseSql(dateStr);
		final List<Date> dates = (fromStr == null) ? Collections.singletonList(asOf) : getDates(
																								(interval != null) ? interval.intValue()
																													: DEFAULT_INTERVAL,
																								this.dateFormat.parseSql(fromStr),
																								asOf);
		final Map<Integer, TrainingtypesRecord> trainingTypes = this.resourcesByKeys.listTrainingTypes();
		final Map<Integer, List<Integer>> certificatesByTrainingType = this.resourcesByKeys.listTrainingtypesCertificates();
		final List<Integer> certificates = this.resources.listCertificates().getValues(CERTIFICATES.CERT_PK);
		final Map<String, Map<Date, EmployeeStatistics>> employeesStatistics = new HashMap<>();
		final Map<String, Boolean> employeesStatus = this.ctx.selectDistinct(SITES_EMPLOYEES.SIEM_EMPL_FK, EMPLOYEES.EMPL_PERMANENT).from(SITES_EMPLOYEES)
				.join(EMPLOYEES).on(SITES_EMPLOYEES.SIEM_EMPL_FK.eq(EMPLOYEES.EMPL_PK)).where(SITES_EMPLOYEES.SIEM_SITE_FK.eq(site_pk))
				.fetchMap(SITES_EMPLOYEES.SIEM_EMPL_FK, EMPLOYEES.EMPL_PERMANENT);
		for (final String empl_pk : employeesStatus.keySet()) {
			employeesStatistics
					.put(empl_pk, getEmployeeStatsForDates(empl_pk, dateStr, dates, trainingTypes, certificatesByTrainingType));
		}

		final TreeSet<Date> updates = new TreeSet<>(this.ctx.select(UPDATES.UPDT_DATE).from(UPDATES).fetchSet(UPDATES.UPDT_DATE));
		final Map<Date, List<String>> employeesHistory = this.ctx.select(SITES_EMPLOYEES.SIEM_EMPL_FK, UPDATES.UPDT_DATE)
				.from(SITES_EMPLOYEES).join(UPDATES).on(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(UPDATES.UPDT_PK)).where(SITES_EMPLOYEES.SIEM_SITE_FK.eq(site_pk))
				.fetchGroups(UPDATES.UPDT_DATE, SITES_EMPLOYEES.SIEM_EMPL_FK);

		final Map<Date, SiteStatistics> res = new TreeMap<>();
		for (final Date date : dates) {
			final SiteStatistics stats = new SiteStatistics(certificates);
			final Date mostAccurate = updates.floor(date);
			if (mostAccurate != null) {
				for (final String empl_pk : employeesHistory.getOrDefault(mostAccurate, Collections.EMPTY_LIST)) {
					stats.register(empl_pk, employeesStatus.get(empl_pk), employeesStatistics.get(empl_pk).get(date));
				}
			}

			res.put(date, stats);
		}

		return res;
	}

	@GET
	@Path("employees")
	public Map<Date, Map<String, EmployeeStatistics>> statsEmployees(
																		@QueryParam("site") final String site_pk,
																		@QueryParam("date") final String dateStr,
																		@QueryParam("from") final String fromStr,
																		@QueryParam("interval") final Integer interval) throws ParseException {
		final Date asOf = (dateStr == null) ? new Date(new java.util.Date().getTime()) : this.dateFormat.parseSql(dateStr);
		final List<Date> dates = (fromStr == null) ? Collections.singletonList(asOf) : getDates(
																								(interval != null) ? interval.intValue()
																													: DEFAULT_INTERVAL,
																								this.dateFormat.parseSql(fromStr),
																								asOf);
		final Map<Integer, TrainingtypesRecord> trainingTypes = this.resourcesByKeys.listTrainingTypes();
		final Map<Integer, List<Integer>> certificatesByTrainingType = this.resourcesByKeys.listTrainingtypesCertificates();
		final Map<Date, Map<String, EmployeeStatistics>> res = new TreeMap<>();
		for (final String rNumber : this.resources.listEmployees(site_pk, dateStr, null).getValues(EMPLOYEES.EMPL_PK)) {
			getEmployeeStatsForDates(rNumber, dateStr, dates, trainingTypes, certificatesByTrainingType)
					.forEach((date, stats) -> res.computeIfAbsent(date, unused -> new HashMap<>()).put(rNumber, stats));
		}

		return res;
	}

	@GET
	@Path("employees/{empl_pk}")
	public Map<Date, EmployeeStatistics> statsEmployee(
														@PathParam("empl_pk") final String empl_pk,
														@QueryParam("date") final String dateStr,
														@QueryParam("from") final String fromStr,
														@QueryParam("interval") final Integer interval) throws ParseException {
		final Date asOf = (dateStr == null) ? new Date(new java.util.Date().getTime()) : this.dateFormat.parseSql(dateStr);
		return getEmployeeStatsForDates(
										empl_pk,
										dateStr,
										(fromStr == null) ? Collections.singletonList(asOf) : getDates((interval != null) ? interval.intValue()
																															: DEFAULT_INTERVAL, this.dateFormat
												.parseSql(fromStr), asOf),
										this.resourcesByKeys.listTrainingTypes(), this.resourcesByKeys.listTrainingtypesCertificates());
	}

	private Map<Date, EmployeeStatistics> getEmployeeStatsForDates(
																	final String empl_pk,
																	final String dateStr,
																	final Iterable<Date> datesList,
																	final Map<Integer, TrainingtypesRecord> trainingTypes,
																	final Map<Integer, List<Integer>> certificatesByTrainingType) throws ParseException {
		final Iterator<Date> dates = datesList.iterator();
		final Map<Date, EmployeeStatistics> res = new TreeMap<>();
		final EmployeeStatisticsBuilder builder = EmployeeStatistics.builder(trainingTypes, certificatesByTrainingType);
		Date nextStop = dates.next();
		for (final Record training : this.resources.listTrainings(empl_pk, Collections.EMPTY_LIST, null, null, dateStr)) {
			while (training.getValue(TRAININGS.TRNG_DATE).after(nextStop)) {
				res.put(nextStop, builder.buildFor(nextStop));
				nextStop = dates.next();
			}

			builder.accept(training);
		}

		res.put(nextStop, builder.buildFor(nextStop));
		while (dates.hasNext()) {
			res.put(nextStop = dates.next(), builder.buildFor(nextStop));
		}

		return res;
	}

	private static List<Date> getDates(final int interval, final Date from, final Date to) {
		final Calendar calendar = Calendar.getInstance();
		final List<Date> res = new ArrayList<>();
		calendar.setTime(from);
		while (calendar.getTime().before(to)) {
			res.add(new java.sql.Date(calendar.getTime().getTime()));
			calendar.add(Calendar.MONTH, interval);
		}

		res.add(to);
		return res;
	}
}