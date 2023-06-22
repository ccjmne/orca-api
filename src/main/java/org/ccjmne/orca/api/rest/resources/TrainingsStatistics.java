package org.ccjmne.orca.api.rest.resources;

import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGS;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.ccjmne.orca.api.rest.resources.TrainingsStatistics.TrainingsCertificateStatistics.TrainingsStatisticsData;
import org.ccjmne.orca.api.utils.StatisticsHelper;
import org.jooq.Record;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;

public class TrainingsStatistics {

	public static class TrainingsStatisticsBuilder {

		private final Map<Integer, Set<Integer>> certificatesByTrainingType;
		private final Map<Integer, TrainingsCertificateStatistics> certificates;
		private Range<LocalDate> dateRange;

		protected TrainingsStatisticsBuilder(final Map<Integer, Set<Integer>> certificatesByTrainingType, final Range<LocalDate> dateRange) {
			this.certificatesByTrainingType = certificatesByTrainingType;
			this.certificates = new HashMap<>();
			this.dateRange = dateRange;
		}

		public void registerTraining(final Record training) {
			for (final Integer certPk : this.certificatesByTrainingType.get(training.getValue(TRAININGS.TRNG_TRTY_FK))) {
				final TrainingsCertificateStatistics certStats = this.certificates
						.getOrDefault(certPk, new TrainingsCertificateStatistics());
				this.certificates.putIfAbsent(certPk, certStats);
				certStats.statistics.trainings += 1;
				certStats.statistics.employeesRegistered += training.getValue(StatisticsHelper.TRAINING_REGISTERED).intValue();
				certStats.statistics.employeesTrained += training.getValue(StatisticsHelper.TRAINING_VALIDATED).intValue();

				final TrainingsStatisticsData typeStats = certStats.trainingTypesStatistics
						.getOrDefault(training.getValue(TRAININGS.TRNG_TRTY_FK), new TrainingsStatisticsData());
				certStats.trainingTypesStatistics.putIfAbsent(training.getValue(TRAININGS.TRNG_TRTY_FK), typeStats);
				typeStats.trainings += 1;
				typeStats.employeesRegistered += training.getValue(StatisticsHelper.TRAINING_REGISTERED).intValue();
				typeStats.employeesTrained += training.getValue(StatisticsHelper.TRAINING_VALIDATED).intValue();
			}
		}

		public void registerExpiry(final Record training) {
			for (final Integer certPk : this.certificatesByTrainingType.get(training.getValue(TRAININGS.TRNG_TRTY_FK))) {
				final TrainingsCertificateStatistics certStats = this.certificates
						.getOrDefault(certPk, new TrainingsCertificateStatistics());
				this.certificates.putIfAbsent(certPk, certStats);
				certStats.statistics.trainingsExpired += 1;
				certStats.statistics.employeesExpired += training.getValue(StatisticsHelper.TRAINING_VALIDATED).intValue();

				final TrainingsStatisticsData typeStats = certStats.trainingTypesStatistics
						.getOrDefault(training.getValue(TRAININGS.TRNG_TRTY_FK), new TrainingsStatisticsData());
				certStats.trainingTypesStatistics.putIfAbsent(training.getValue(TRAININGS.TRNG_TRTY_FK), typeStats);
				typeStats.trainingsExpired += 1;
				typeStats.employeesExpired += training.getValue(StatisticsHelper.TRAINING_VALIDATED).intValue();
			}
		}

		public TrainingsStatistics build() {
			return new TrainingsStatistics(getBeginning(), getEnd(), this.certificates);
		}

		public LocalDate getBeginning() {
			return this.dateRange.lowerEndpoint();
		}

		public LocalDate getEnd() {
			if (this.dateRange.upperBoundType().equals(BoundType.OPEN)) {
				return this.dateRange.upperEndpoint().minusDays(1);
			}

			return this.dateRange.upperEndpoint();
		}

		public void closeRange() {
			this.dateRange = Range.<LocalDate> closed(this.dateRange.lowerEndpoint(), this.dateRange.upperEndpoint());
		}

		public Range<LocalDate> getDateRange() {
			return this.dateRange;
		}
	}

	public static TrainingsStatisticsBuilder builder(final Map<Integer, Set<Integer>> certificatesByTrainingType, final Range<LocalDate> dateRange) {
		return new TrainingsStatisticsBuilder(certificatesByTrainingType, dateRange);
	}

	private final LocalDate beginning;
	private final LocalDate end;
	private final Map<Integer, TrainingsCertificateStatistics> certificates;

	public TrainingsStatistics(final LocalDate beginning, final LocalDate end, final Map<Integer, TrainingsCertificateStatistics> certificates) {
		this.beginning = beginning;
		this.end = end;
		this.certificates = certificates;
	}

	public LocalDate getBeginning() {
		return this.beginning;
	}

	public LocalDate getEnd() {
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
