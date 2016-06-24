package org.ccjmne.faomaintenance.api.rest.resources;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS;

import java.sql.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ccjmne.faomaintenance.api.rest.resources.TrainingsStatistics.TrainingsCertificateStatistics.TrainingsStatisticsData;
import org.ccjmne.faomaintenance.api.utils.Constants;
import org.jooq.Record;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;

public class TrainingsStatistics {

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
				final TrainingsCertificateStatistics trainingCertificatesStatistics = this.certificates
						.getOrDefault(certPk, new TrainingsCertificateStatistics());
				this.certificates.putIfAbsent(certPk, trainingCertificatesStatistics);
				trainingCertificatesStatistics.statistics.trainings += 1;
				trainingCertificatesStatistics.statistics.employeesRegistered += training.getValue(Constants.AGENTS_REGISTERED).intValue();
				trainingCertificatesStatistics.statistics.employeesTrained += training.getValue(Constants.AGENTS_VALIDATED).intValue();
				final TrainingsStatisticsData trainingTypeStatistics = trainingCertificatesStatistics.trainingTypesStatistics
						.getOrDefault(training.getValue(TRAININGS.TRNG_TRTY_FK), new TrainingsStatisticsData());
				trainingCertificatesStatistics.trainingTypesStatistics.putIfAbsent(training.getValue(TRAININGS.TRNG_TRTY_FK), trainingTypeStatistics);
				trainingTypeStatistics.trainings += 1;
				trainingTypeStatistics.employeesRegistered += training.getValue(Constants.AGENTS_REGISTERED).intValue();
				trainingTypeStatistics.employeesTrained += training.getValue(Constants.AGENTS_VALIDATED).intValue();
			}
		}

		public void registerExpiry(final Record training) {
			for (final Integer certPk : this.certificatesByTrainingType.get(training.getValue(TRAININGS.TRNG_TRTY_FK))) {
				final TrainingsCertificateStatistics trainingCertificatesStatistics = this.certificates
						.getOrDefault(certPk, new TrainingsCertificateStatistics());
				this.certificates.putIfAbsent(certPk, trainingCertificatesStatistics);
				trainingCertificatesStatistics.statistics.trainingsExpired += 1;
				trainingCertificatesStatistics.statistics.employeesExpired += training.getValue(Constants.AGENTS_VALIDATED).intValue();
				final TrainingsStatisticsData trainingTypeStatistics = trainingCertificatesStatistics.trainingTypesStatistics
						.getOrDefault(training.getValue(TRAININGS.TRNG_TRTY_FK), new TrainingsStatisticsData());
				trainingCertificatesStatistics.trainingTypesStatistics.putIfAbsent(training.getValue(TRAININGS.TRNG_TRTY_FK), trainingTypeStatistics);
				trainingTypeStatistics.trainingsExpired += 1;
				trainingTypeStatistics.employeesExpired += training.getValue(Constants.AGENTS_VALIDATED).intValue();
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
			this.dateRange = Range.<Date> closed(this.dateRange.lowerEndpoint(), this.dateRange.upperEndpoint());
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

	public Date getBeginning() {
		return this.beginning;
	}

	public Date getEnd() {
		return this.end;
	}

	public Map<Integer, TrainingsCertificateStatistics> getCertificates() {
		return this.certificates;
	}

	public static class TrainingsCertificateStatistics {
		public static class TrainingsStatisticsData {
			public int trainings;
			public int trainingsExpired;
			public int employeesRegistered;
			public int employeesTrained;
			public int employeesExpired;

		}

		public final TrainingsStatisticsData statistics;
		public final Map<Integer, TrainingsStatisticsData> trainingTypesStatistics;

		public TrainingsCertificateStatistics() {
			this.statistics = new TrainingsStatisticsData();
			this.trainingTypesStatistics = new HashMap<>();
		}
	}
}
