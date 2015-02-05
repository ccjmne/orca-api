package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGTYPES;
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

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.ccjmne.faomaintenance.api.rest.resources.EmployeeStatistics;
import org.ccjmne.faomaintenance.api.rest.resources.EmployeeStatistics.EmployeeStatisticsBuilder;
import org.ccjmne.faomaintenance.api.rest.resources.SiteStatistics;
import org.ccjmne.faomaintenance.api.utils.SQLDateFormat;
import org.ccjmne.faomaintenance.jooq.classes.Tables;
import org.jooq.DSLContext;
import org.jooq.Record;

@Path("statistics")
public class StatisticsEndpoint {

	private static final int DEFAULT_INTERVAL = 6;
	private final ResourcesEndpoint resources;
	private final SQLDateFormat dateFormat;
	private final DSLContext ctx;

	@Inject
	public StatisticsEndpoint(final DSLContext ctx, final SQLDateFormat dateFormat, final ResourcesEndpoint resources) {
		this.ctx = ctx;
		this.dateFormat = dateFormat;
		this.resources = resources;
	}

	@GET
	@Path("sites")
	public Map<Date, Map<String, SiteStatistics>> statsSites(
																@QueryParam("dept") final Integer department,
																@QueryParam("date") final String dateStr,
																@QueryParam("from") final String fromStr,
																@QueryParam("interval") final Integer interval) throws ParseException {
		final Date asOf = (dateStr == null) ? new Date(new java.util.Date().getTime()) : this.dateFormat.parseSql(dateStr);
		final List<Date> dates = (fromStr == null) ? Collections.singletonList(asOf) : getDates(
																								(interval != null) ? interval.intValue() : DEFAULT_INTERVAL,
																								this.dateFormat.parseSql(fromStr),
																								asOf);
		final Map<Integer, Record> trainingTypes = this.resources.listTrainingTypes().intoMap(TRAININGTYPES.TRTY_PK);
		final Map<String, Map<Date, EmployeeStatistics>> employeesStatistics = new HashMap<>();
		final List<String> sites = this.resources.listSites(department).getValues(SITES.SITE_PK);
		final Map<String, Boolean> employees = this.ctx.selectDistinct(SITES_EMPLOYEES.SIEM_EMPL_FK, Tables.EMPLOYEES.EMPL_PERMANENT).from(SITES_EMPLOYEES)
				.join(Tables.EMPLOYEES)
				.on(SITES_EMPLOYEES.SIEM_EMPL_FK.eq(Tables.EMPLOYEES.EMPL_PK))
				.where(SITES_EMPLOYEES.SIEM_SITE_FK.in(sites)).fetchMap(SITES_EMPLOYEES.SIEM_EMPL_FK, Tables.EMPLOYEES.EMPL_PERMANENT);
		for (final String registrationNumber : employees.keySet()) {
			employeesStatistics.put(registrationNumber, getEmployeeStatsForDates(registrationNumber, dateStr, dates, trainingTypes));
		}

		final TreeMap<Date, Map<String, List<String>>> employeesHistory = new TreeMap<>();
		for (final String aurore : sites) {
			new TreeMap<>(this.ctx.select(SITES_EMPLOYEES.SIEM_EMPL_FK, UPDATES.UPDT_DATE).from(SITES_EMPLOYEES).join(UPDATES)
					.on(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(UPDATES.UPDT_PK)).where(SITES_EMPLOYEES.SIEM_SITE_FK.eq(aurore))
					.fetchGroups(UPDATES.UPDT_DATE, SITES_EMPLOYEES.SIEM_EMPL_FK)).forEach((date, siteEmployees) -> employeesHistory
					.computeIfAbsent(date, unused -> new HashMap<>()).put(aurore, siteEmployees));
		}

		final Map<Date, Map<String, SiteStatistics>> res = new TreeMap<>();
		for (final Date date : dates) {
			final Entry<Date, Map<String, List<String>>> mostAccurate = employeesHistory.floorEntry(date);
			if (mostAccurate != null) {
				for (final Entry<String, List<String>> siteHistory : mostAccurate.getValue().entrySet()) {
					final SiteStatistics stats = new SiteStatistics();
					siteHistory.getValue().forEach(
													registrationNumber -> stats.register(
																							employees.get(registrationNumber),
																							employeesStatistics.get(registrationNumber).get(date)));
					res.computeIfAbsent(date, unused -> new HashMap<>()).put(siteHistory.getKey(), stats);
				}
			} else {
				res.put(date, Collections.EMPTY_MAP);
			}
		}

		return res;
	}

	@GET
	@Path("sites/{aurore}")
	public Map<Date, SiteStatistics> statsSite(
												@PathParam("aurore") final String aurore,
												@QueryParam("date") final String dateStr,
												@QueryParam("from") final String fromStr,
												@QueryParam("interval") final Integer interval) throws ParseException {
		final Date asOf = (dateStr == null) ? new Date(new java.util.Date().getTime()) : this.dateFormat.parseSql(dateStr);
		final List<Date> dates = (fromStr == null) ? Collections.singletonList(asOf) : getDates(
																								(interval != null) ? interval.intValue()
																													: DEFAULT_INTERVAL,
																								this.dateFormat.parseSql(fromStr),
																								asOf);
		final Map<Integer, Record> trainingTypes = this.resources.listTrainingTypes().intoMap(TRAININGTYPES.TRTY_PK);
		final Map<String, Map<Date, EmployeeStatistics>> employeesStatistics = new HashMap<>();
		final Map<String, Boolean> employees = this.ctx.selectDistinct(SITES_EMPLOYEES.SIEM_EMPL_FK, Tables.EMPLOYEES.EMPL_PERMANENT).from(SITES_EMPLOYEES)
				.join(Tables.EMPLOYEES)
				.on(SITES_EMPLOYEES.SIEM_EMPL_FK.eq(Tables.EMPLOYEES.EMPL_PK)).where(SITES_EMPLOYEES.SIEM_SITE_FK.eq(aurore))
				.fetchMap(SITES_EMPLOYEES.SIEM_EMPL_FK, Tables.EMPLOYEES.EMPL_PERMANENT);
		for (final String registrationNumber : employees.keySet()) {
			employeesStatistics.put(registrationNumber, getEmployeeStatsForDates(registrationNumber, dateStr, dates, trainingTypes));
		}

		final TreeMap<Date, List<String>> employeesHistory = new TreeMap<>(this.ctx.select(SITES_EMPLOYEES.SIEM_EMPL_FK, UPDATES.UPDT_DATE)
				.from(SITES_EMPLOYEES)
				.join(UPDATES).on(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(UPDATES.UPDT_PK)).where(SITES_EMPLOYEES.SIEM_SITE_FK.eq(aurore))
				.fetchGroups(UPDATES.UPDT_DATE, SITES_EMPLOYEES.SIEM_EMPL_FK));

		final Map<Date, SiteStatistics> res = new TreeMap<>();
		for (final Date date : dates) {
			final SiteStatistics stats = new SiteStatistics();
			final Entry<Date, List<String>> mostAccurate = employeesHistory.floorEntry(date);
			if (mostAccurate != null) {
				for (final String registrationNumber : mostAccurate.getValue()) {
					stats.register(employees.get(registrationNumber), employeesStatistics.get(registrationNumber).get(date));
				}
			}

			res.put(date, stats);
		}

		return res;
	}

	@GET
	@Path("employees/{registrationNumber}")
	public Map<Date, EmployeeStatistics> statsEmployee(
														@PathParam("registrationNumber") final String registrationNumber,
														@QueryParam("date") final String dateStr,
														@QueryParam("from") final String fromStr,
														@QueryParam("interval") final Integer interval) throws ParseException {
		final Date asOf = (dateStr == null) ? new Date(new java.util.Date().getTime()) : this.dateFormat.parseSql(dateStr);
		return getEmployeeStatsForDates(
										registrationNumber,
										dateStr,
										(fromStr == null) ? Collections.singletonList(asOf) : getDates((interval != null) ? interval.intValue()
																															: DEFAULT_INTERVAL, this.dateFormat
												.parseSql(fromStr), asOf),
										this.resources.listTrainingTypes().intoMap(TRAININGTYPES.TRTY_PK));
	}

	private Map<Date, EmployeeStatistics> getEmployeeStatsForDates(
																	final String registrationNumber,
																	final String dateStr,
																	final Iterable<Date> datesList,
																	final Map<Integer, Record> trainingTypes) throws ParseException {
		final Iterator<Date> dates = datesList.iterator();
		final Map<Date, EmployeeStatistics> res = new TreeMap<>();
		final EmployeeStatisticsBuilder builder = EmployeeStatistics.builder(trainingTypes);
		Date nextStop = dates.next();
		for (final Record training : this.resources.listTrainings(registrationNumber, Collections.EMPTY_LIST, null, null, dateStr)) {
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