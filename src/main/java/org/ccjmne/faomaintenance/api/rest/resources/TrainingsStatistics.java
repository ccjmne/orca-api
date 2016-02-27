package org.ccjmne.faomaintenance.api.rest.resources;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGTYPES;

import java.sql.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jooq.DatePart;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.impl.DSL;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;

public class TrainingsStatistics {

	public static final Field<Integer> AGENTS_REGISTERED = DSL.count(TRAININGS_EMPLOYEES.TREM_PK).as("registered");
	public static final Field<Integer> AGENTS_VALIDATED = DSL.count(TRAININGS_EMPLOYEES.TREM_OUTCOME)
			.filterWhere(TRAININGS_EMPLOYEES.TREM_OUTCOME.eq("VALIDATED")).as("validated");
	public static final Field<Date> EXPIRY_DATE = DSL
			.dateAdd(
						TRAININGS.TRNG_DATE,
						DSL.field(DSL.select(TRAININGTYPES.TRTY_VALIDITY).from(TRAININGTYPES).where(TRAININGTYPES.TRTY_PK.eq(TRAININGS.TRNG_TRTY_FK))),
						DatePart.MONTH);

	public static class TrainingsStatisticsBuilder {
		private final Map<Integer, List<Integer>> certificatesByTrainingType;
		private final Map<Integer, TrainingsCertificateStatistics> certificates;
		private Range<Date> dateRange;

		protected TrainingsStatisticsBuilder(final Map<Integer, List<Integer>> certificatesByTrainingType, final Range<Date> dateRange) {
			this.certificatesByTrainingType = certificatesByTrainingType;
			this.certificates = new HashMap<>();
			this.dateRange = dateRange;
		}

		public void registerTraining(final Record training) {
			for (final Integer certPk : this.certificatesByTrainingType.get(training.getValue(TRAININGS.TRNG_TRTY_FK))) {
				this.certificates.merge(
										certPk,
										new TrainingsCertificateStatistics(
																			1,
																			0,
																			training.getValue(TrainingsStatistics.AGENTS_REGISTERED).intValue(),
																			training.getValue(TrainingsStatistics.AGENTS_VALIDATED).intValue(),
																			0),
										TrainingsCertificateStatistics::merge);
			}
		}

		public void registerExpiry(final Record training) {
			for (final Integer certPk : this.certificatesByTrainingType.get(training.getValue(TRAININGS.TRNG_TRTY_FK))) {
				this.certificates.merge(
										certPk,
										new TrainingsCertificateStatistics(0, 1, 0, 0, training.getValue(TrainingsStatistics.AGENTS_VALIDATED).intValue()),
										TrainingsCertificateStatistics::merge);
			}
		}

		public TrainingsStatistics build() {
			return new TrainingsStatistics(getBeginning(), getEnd(), this.certificates);
		}

		public Date getBeginning() {
			return this.dateRange.lowerEndpoint();
		}

		public Date getEnd() {
			if (this.dateRange.upperBoundType().equals(BoundType.OPEN)) {
				return Date.valueOf(this.dateRange.upperEndpoint().toLocalDate().minusDays(1));
			}

			return this.dateRange.upperEndpoint();
		}

		public void closeRange() {
			this.dateRange = Range.<Date> closed(getBeginning(), getEnd());
		}

		public Range<Date> getDateRange() {
			return this.dateRange;
		}
	}

	public static TrainingsStatisticsBuilder builder(
														final Map<Integer, List<Integer>> certificatesByTrainingType,
														final Range<Date> dateRange) {
		return new TrainingsStatisticsBuilder(certificatesByTrainingType, dateRange);
	}

	private final Date beginning;
	private final Date end;
	private final Map<Integer, TrainingsCertificateStatistics> certificates;

	public TrainingsStatistics(final Date beginning, final Date end, final Map<Integer, TrainingsCertificateStatistics> certificates) {
		this.beginning = beginning;
		this.end = end;
		this.certificates = certificates;
	}

	public static class TrainingsCertificateStatistics {
		public final int trainings;
		public final int trainingsExpired;
		public final int employeesRegistered;
		public final int employeesTrained;
		public final int employeesExpired;

		public TrainingsCertificateStatistics(
												final int trainings,
												final int trainingsExpired,
												final int employeesRegistered,
												final int employeesTrained,
												final int employeesExpired) {
			this.trainings = trainings;
			this.trainingsExpired = trainingsExpired;
			this.employeesRegistered = employeesRegistered;
			this.employeesTrained = employeesTrained;
			this.employeesExpired = employeesExpired;
		}

		protected static TrainingsCertificateStatistics merge(final TrainingsCertificateStatistics a, final TrainingsCertificateStatistics b) {
			return new TrainingsCertificateStatistics(
														a.trainings + b.trainings,
														a.trainingsExpired + b.trainingsExpired,
														a.employeesRegistered + b.employeesRegistered,
														a.employeesTrained + b.employeesTrained,
														a.employeesExpired + b.employeesExpired);
		}
	}

	public Date getBeginning() {
		return this.beginning;
	}

	public Date getEnd() {
		return this.end;
	}

	public Map<Integer, TrainingsCertificateStatistics> getCertificates() {
		return this.certificates;
	}
}
