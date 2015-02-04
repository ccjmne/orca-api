package org.ccjmne.faomaintenance.api.rest;

import java.sql.Date;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.ccjmne.faomaintenance.api.rest.EmployeeStatistics.EmployeeStatisticsBuilder;
import org.ccjmne.faomaintenance.api.utils.SQLDateFormat;
import org.ccjmne.faomaintenance.jooq.classes.Tables;
import org.jooq.Record;

@Path("statistics")
public class StatisticsEndpoint {

	private static final int DEFAULT_INTERVAL = 6;
	private final ResourcesEndpoint resources;
	private final SQLDateFormat dateFormat;

	@Inject
	public StatisticsEndpoint(final SQLDateFormat dateFormat, final ResourcesEndpoint resources) {
		this.dateFormat = dateFormat;
		this.resources = resources;
	}

	@GET
	@Path("employees/{registrationNumber}")
	public Map<Date, EmployeeStatistics> statsEmployee(
														@PathParam("registrationNumber") final String registrationNumber,
														@QueryParam("date") final String dateStr,
														@QueryParam("from") final String fromStr,
														@QueryParam("interval") final Integer interval) throws ParseException {
		final Date asOf = (dateStr == null) ? new Date(new java.util.Date().getTime()) : this.dateFormat.parseSql(dateStr);
		final Iterator<Date> dates = (fromStr == null) ? Collections.singletonList(asOf).iterator() : getDates(
																												(interval != null) ? interval.intValue()
																																	: DEFAULT_INTERVAL,
																												this.dateFormat.parseSql(fromStr),
																												asOf);
		final Map<Date, EmployeeStatistics> res = new TreeMap<>();
		final EmployeeStatisticsBuilder builder = EmployeeStatistics.builder(this.resources.listTrainingTypes().intoMap(Tables.TRAININGTYPES.TRTY_PK));
		Date nextStop = dates.next();
		for (final Record training : this.resources.listTrainings(registrationNumber, Collections.EMPTY_LIST, null, null, dateStr)) {
			while (training.getValue(Tables.TRAININGS.TRNG_DATE).after(nextStop)) {
				res.put(nextStop, builder.buildFor(nextStop));
				nextStop = dates.next();
			}

			builder.accept(training);
		}

		while (dates.hasNext()) {
			res.put(nextStop = dates.next(), builder.buildFor(nextStop));
		}

		return res;
	}

	private static Iterator<Date> getDates(final int interval, final Date from, final Date to) {
		final Calendar calendar = Calendar.getInstance();
		final List<Date> res = new ArrayList<>();
		calendar.setTime(from);
		while (calendar.getTime().before(to)) {
			res.add(new java.sql.Date(calendar.getTime().getTime()));
			calendar.add(Calendar.MONTH, interval);
		}

		res.add(to);
		return res.iterator();
	}
}
