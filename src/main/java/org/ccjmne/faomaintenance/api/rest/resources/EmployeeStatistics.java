package org.ccjmne.faomaintenance.api.rest.resources;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGTYPES;

import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ccjmne.faomaintenance.jooq.classes.tables.records.TrainingtypesRecord;
import org.jooq.Record;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Multimap;

public class EmployeeStatistics {

	private static final String OUTCOME_VALID = "VALIDATED";

	public static class EmployeeStatisticsBuilder {

		private final Map<Integer, TrainingtypesRecord> trainingTypes;
		private final Map<Integer, java.util.Date> expiryDates;
		private final Calendar calendar;
		private final Map<Integer, List<Integer>> certificatesByTrainingType;
		private final Map<Integer, java.util.Date> certificatesVoiding;
		private final Multimap<Integer, Record> trainings;

		protected EmployeeStatisticsBuilder(
											final Map<Integer, TrainingtypesRecord> trainingTypes,
											final Map<Integer, List<Integer>> certificatesByTrainingType,
											final Map<Integer, Date> certificatesVoiding) {
			this.trainingTypes = trainingTypes;
			this.certificatesByTrainingType = certificatesByTrainingType;
			this.calendar = Calendar.getInstance();
			this.expiryDates = new HashMap<>();
			this.trainings = ArrayListMultimap.<Integer, Record> create();
			this.certificatesVoiding = certificatesVoiding;
		}

		public EmployeeStatistics.EmployeeStatisticsBuilder accept(final Record training) {
			if (OUTCOME_VALID.equals(training.getValue(TRAININGS_EMPLOYEES.TREM_OUTCOME))) {
				final Record trainingType = this.trainingTypes.get(training.getValue(TRAININGS.TRNG_TRTY_FK));
				final java.sql.Date trainingDate = training.getValue(TRAININGS.TRNG_DATE);
				this.calendar.setTime(trainingDate);
				this.calendar.add(Calendar.MONTH, trainingType.getValue(TRAININGTYPES.TRTY_VALIDITY).intValue());
				for (final Integer cert_pk : this.certificatesByTrainingType.get(training.getValue(TRAININGS.TRNG_TRTY_FK))) {
					this.trainings.put(cert_pk, training);
					this.expiryDates.merge(cert_pk, this.calendar.getTime(), (expiryDate, potential) -> (potential.after(expiryDate)) ? potential : expiryDate);
					if (this.certificatesVoiding.containsKey(cert_pk)) {
						this.expiryDates.merge(
												cert_pk,
												this.certificatesVoiding.get(cert_pk),
												(expiryDate, voidingDate) -> (voidingDate.after(expiryDate)) ? expiryDate : voidingDate);
					}

				}
			}

			return this;
		}

		public EmployeeStatistics buildFor(final Date asOf) {
			return new EmployeeStatistics(this.expiryDates, this.trainings, asOf);
		}
	}

	public static EmployeeStatistics.EmployeeStatisticsBuilder builder(
																		final Map<Integer, TrainingtypesRecord> trainingTypes,
																		final Map<Integer, List<Integer>> certificatesByTrainingTypes,
																		final Map<Integer, java.util.Date> certificatesVoiding) {
		return new EmployeeStatisticsBuilder(trainingTypes, certificatesByTrainingTypes, certificatesVoiding);
	}

	private final Map<Integer, EmployeeCertificateStatistics> certificates;
	private final Date asOf;

	protected EmployeeStatistics(final Map<Integer, java.util.Date> expiryDates, final Multimap<Integer, Record> trainings, final Date asOf) {
		this.asOf = asOf;
		final Builder<Integer, EmployeeCertificateStatistics> builder = ImmutableMap.<Integer, EmployeeCertificateStatistics> builder();
		expiryDates.forEach((certificate, expiryDate) -> builder.put(certificate, new EmployeeCertificateStatistics(expiryDate, trainings.get(certificate))));
		this.certificates = builder.build();
	}

	@JsonIgnore
	public Date getAsOf() {
		return this.asOf;
	}

	public Map<Integer, EmployeeCertificateStatistics> getCertificates() {
		return this.certificates;
	}

	public class EmployeeCertificateStatistics {

		private final java.util.Date expiryDate;
		private final boolean valid;
		private final boolean validForAWhile;
		private final Collection<Record> trainings;

		protected EmployeeCertificateStatistics(final java.util.Date expiryDate, final Collection<Record> trainings) {
			this.expiryDate = expiryDate;
			this.valid = this.expiryDate.after(getAsOf());
			final Calendar instance = Calendar.getInstance();
			instance.setTime(getAsOf());
			instance.add(Calendar.MONTH, 6);
			this.validForAWhile = this.expiryDate.after(instance.getTime());
			this.trainings = trainings;
		}

		public java.util.Date getExpiryDate() {
			return this.expiryDate;
		}

		public boolean isValid() {
			return this.valid;
		}

		public boolean isValidForAWhile() {
			return this.validForAWhile;
		}

		public String getValidityStatus() {
			if (isValidForAWhile()) {
				return "success";
			}

			if (isValid()) {
				return "warning";
			}

			return "danger";
		}

		public Collection<Record> getTrainings() {
			return this.trainings;
		}
	}
}