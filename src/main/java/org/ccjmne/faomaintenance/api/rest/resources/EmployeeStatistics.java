package org.ccjmne.faomaintenance.api.rest.resources;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import jersey.repackaged.com.google.common.collect.ImmutableMap;
import jersey.repackaged.com.google.common.collect.ImmutableMap.Builder;

import org.ccjmne.faomaintenance.jooq.classes.Tables;
import org.jooq.Record;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;

public class EmployeeStatistics {

	public static class EmployeeStatisticsBuilder {

		private final Map<Integer, Record> trainingTypes;
		private final Map<Integer, java.util.Date> expiryDates;
		private final Calendar calendar;

		protected EmployeeStatisticsBuilder(final Map<Integer, Record> trainingTypes) {
			this.trainingTypes = trainingTypes;
			this.calendar = Calendar.getInstance();
			this.expiryDates = new HashMap<>();
		}

		public EmployeeStatistics.EmployeeStatisticsBuilder accept(final Record training) {
			if (training.getValue(Tables.TRAININGS_EMPLOYEES.TREM_VALID).booleanValue()) {
				final Record trainingType = this.trainingTypes.get(training.getValue(Tables.TRAININGS.TRNG_TRTY_FK));
				this.calendar.setTime(training.getValue(Tables.TRAININGS.TRNG_DATE));
				this.calendar.add(Calendar.MONTH, trainingType.getValue(Tables.TRAININGTYPES.TRTY_VALIDITY).intValue());
				this.expiryDates.merge(
										trainingType.getValue(Tables.TRAININGTYPES.TRTY_CERT_FK),
										this.calendar.getTime(),
										(expiryDate, potential) -> (potential.after(expiryDate)) ? potential : expiryDate);
			}

			return this;
		}

		public EmployeeStatistics buildFor(final Date asOf) {
			return new EmployeeStatistics(this.expiryDates, asOf);
		}
	}

	public static EmployeeStatistics.EmployeeStatisticsBuilder builder(final Map<Integer, Record> trainingTypes) {
		return new EmployeeStatisticsBuilder(trainingTypes);
	}

	private final Map<Integer, EmployeeCertificateStatistics> statistics;
	private final Date asOf;

	protected EmployeeStatistics(final Map<Integer, java.util.Date> expiryDates, final Date asOf) {
		final Builder<Integer, EmployeeCertificateStatistics> builder = ImmutableMap.<Integer, EmployeeCertificateStatistics> builder();
		expiryDates.forEach((certificate, expiryDate) -> builder.put(certificate, new EmployeeCertificateStatistics(expiryDate)));
		this.statistics = builder.build();
		this.asOf = asOf;
	}

	@JsonIgnore
	public Date getAsOf() {
		return this.asOf;
	}

	@JsonAnyGetter
	public Map<Integer, EmployeeCertificateStatistics> getStatistics() {
		return this.statistics;
	}

	public class EmployeeCertificateStatistics {

		private final java.util.Date expiryDate;

		protected EmployeeCertificateStatistics(final java.util.Date expiryDate) {
			this.expiryDate = expiryDate;
		}

		public java.util.Date getExpiryDate() {
			return this.expiryDate;
		}

		public boolean isValid() {
			return this.expiryDate.after(getAsOf());
		}
	}
}