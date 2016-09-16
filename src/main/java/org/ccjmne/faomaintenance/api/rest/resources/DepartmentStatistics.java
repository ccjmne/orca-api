package org.ccjmne.faomaintenance.api.rest.resources;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.CERTIFICATES;

import java.util.Map;

import org.ccjmne.faomaintenance.api.rest.resources.SiteStatistics.SiteCertificateStatistics;
import org.ccjmne.faomaintenance.api.utils.Constants;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.CertificatesRecord;

import com.google.common.collect.ImmutableMap;

public class DepartmentStatistics {

	private final Map<Integer, DepartmentCertificateStatistics> certificates;
	private int sitesCount;
	private int employeesCount;
	private int permanentsCount;

	public DepartmentStatistics(final Map<Integer, CertificatesRecord> certificates) {
		final ImmutableMap.Builder<Integer, DepartmentCertificateStatistics> builder = ImmutableMap.<Integer, DepartmentCertificateStatistics> builder();
		certificates.forEach((cert_pk, certificate) -> builder.put(cert_pk, new DepartmentCertificateStatistics(certificate)));
		this.certificates = builder.build();
		this.sitesCount = 0;
		this.employeesCount = 0;
		this.permanentsCount = 0;
	}

	public DepartmentStatistics(final Map<Integer, CertificatesRecord> certificates, final Iterable<SiteStatistics> sitesStatistics) {
		this(certificates);
		sitesStatistics.forEach(this::register);
	}

	public void register(final SiteStatistics stats) {
		stats.getCertificates().forEach((cert, stat) -> {
			if (this.certificates.containsKey(cert)) {
				this.certificates.get(cert).register(stat);
			}
		});

		this.sitesCount++;
		this.employeesCount += stats.getEmployeesCount();
		this.permanentsCount += stats.getPermanentsCount();
	}

	public int getSitesCount() {
		return this.sitesCount;
	}

	public int getEmployeesCount() {
		return this.employeesCount;
	}

	public int getPermanentsCount() {
		return this.permanentsCount;
	}

	public Map<Integer, DepartmentCertificateStatistics> getCertificates() {
		return this.certificates;
	}

	public class DepartmentCertificateStatistics {

		private int count;
		private int targetSuccessCount;
		private int targetWarningCount;
		private int targetDangerCount;

		private final int targetPercentage;
		private final CertificatesRecord certificateInformation;

		public DepartmentCertificateStatistics(final CertificatesRecord certificateInformation) {
			this.certificateInformation = certificateInformation;
			this.targetPercentage = this.certificateInformation.getValue(CERTIFICATES.CERT_TARGET).intValue();
			this.count = 0;
			this.targetSuccessCount = 0;
			this.targetWarningCount = 0;
			this.targetDangerCount = 0;
		}

		protected void register(final SiteCertificateStatistics siteCertStat) {
			this.count += siteCertStat.getCount();
			switch (siteCertStat.getTargetStatus()) {
				case Constants.STATUS_SUCCESS:
					this.targetSuccessCount++;
					break;
				case Constants.STATUS_WARNING:
					this.targetWarningCount++;
					break;
				default:
					this.targetDangerCount++;
			}
		}

		public int getCount() {
			return this.count;
		}

		public int getTargetSuccessCount() {
			return this.targetSuccessCount;
		}

		public int getTargetWarningCount() {
			return this.targetWarningCount;
		}

		public int getTargetDangerCount() {
			return this.targetDangerCount;
		}

		public int getTarget() {
			return Double.valueOf(Math.ceil((this.targetPercentage * getEmployeesCount()) / 100.0f)).intValue();
		}

		public int getTargetIndex() {
			return Math.round(((this.targetSuccessCount + ((2 * this.targetWarningCount) / 3f)) / getSitesCount()) * 100f);
		}

		public int getCountPercentage() {
			return getEmployeesCount() > 0 ? (this.count * 100) / getEmployeesCount() : 0;
		}

		public String getTargetStatus() {
			final int index = getTargetIndex();
			if (index < 67) {
				return Constants.STATUS_DANGER;
			}

			if (index < 100) {
				return Constants.STATUS_WARNING;
			}

			return Constants.STATUS_SUCCESS;
		}
	}
}
