package org.ccjmne.faomaintenance.api.rest;

import java.sql.Date;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.ccjmne.faomaintenance.api.jooq.Tables;
import org.ccjmne.faomaintenance.api.utils.SQLDateFormat;
import org.jooq.Record;
import org.jooq.Result;

@Path("statistics")
public class StatisticsEndpoint {

	private final ResourcesEndpoint resources;
	private final SQLDateFormat dateFormat;
	private final Calendar calendar;

	@Inject
	public StatisticsEndpoint(final SQLDateFormat dateFormat, final ResourcesEndpoint resources) {
		this.dateFormat = dateFormat;
		this.resources = resources;
		this.calendar = Calendar.getInstance();
	}

	@GET
	@Path("employees/{registrationNumber}")
	public Map<Integer, EmployeeStatistics> statsEmployee(
															@PathParam("registrationNumber") final String registrationNumber,
															@QueryParam("date") final String dateStr) throws ParseException {
		final Date asOf = (dateStr == null) ? new Date(new java.util.Date().getTime()) : this.dateFormat.parseSql(dateStr);
		final Map<Integer, Record> trainingTypes = this.resources.listTrainingTypes().intoMap(Tables.TRAININGTYPES.TRTY_PK);
		final Result<Record> trainings = this.resources.listTrainings(registrationNumber, Collections.EMPTY_LIST, null, null, dateStr);
		final Map<Integer, EmployeeStatistics> res = new HashMap<>();
		for (final Record training : trainings) {
			final Record trainingType = trainingTypes.get(training.getValue(Tables.TRAININGS.TRNG_TRTY_FK));
			if (training.getValue(Tables.TRAININGS_EMPLOYEES.TREM_VALID).booleanValue()) {
				this.calendar.setTime(training.getValue(Tables.TRAININGS.TRNG_DATE));
				this.calendar.add(Calendar.MONTH, trainingType.getValue(Tables.TRAININGTYPES.TRTY_VALIDITY).intValue());
				res.computeIfAbsent(trainingType.getValue(Tables.TRAININGTYPES.TRTY_CERT_FK), unused -> new EmployeeStatistics(asOf, this.calendar.getTime()))
						.renew(this.calendar.getTime());
			}
		}

		return res;
	}

	public static class EmployeeStatistics {

		private final Date asOf;
		private java.util.Date expiryDate;

		public EmployeeStatistics(final Date asOf, final java.util.Date expiryDate) {
			this.asOf = asOf;
			this.expiryDate = expiryDate;
		}

		public void renew(final java.util.Date renewDate) {
			if (renewDate.after(this.expiryDate)) {
				this.expiryDate = renewDate;
			}
		}

		public Date getAsOf() {
			return this.asOf;
		}

		public java.util.Date getExpiryDate() {
			return this.expiryDate;
		}

		public boolean isValid() {
			return this.expiryDate.after(this.asOf);
		}
	}
}
