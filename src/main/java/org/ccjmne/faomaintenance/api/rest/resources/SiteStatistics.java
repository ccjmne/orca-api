package org.ccjmne.faomaintenance.api.rest.resources;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.CERTIFICATES;

import java.util.HashMap;
import java.util.Map;

import org.ccjmne.faomaintenance.api.rest.resources.EmployeeStatistics.EmployeeCertificateStatistics;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.CertificatesRecord;

import com.google.common.collect.ImmutableMap;

public class SiteStatistics {

	private final Map<Integer, SiteCertificateStatistics> certificates;
	private final Map<String, EmployeeStatistics> employeesStatistics;
	private int employeesCount;
	private int permanentsCount;

	public SiteStatistics(final Map<Integer, CertificatesRecord> certificates) {
		final ImmutableMap.Builder<Integer, SiteCertificateStatistics> builder = ImmutableMap.<Integer, SiteCertificateStatistics> builder();
		certificates.forEach((cert_pk, certificate) -> builder.put(cert_pk, new SiteCertificateStatistics(certificate)));
		this.certificates = builder.build();
		this.employeesStatistics = new HashMap<>();
		this.employeesCount = 0;
		this.permanentsCount = 0;
	}

	public int getEmployeesCount() {
		return this.employeesCount;
	}

	public int getPermanentsCount() {
		return this.permanentsCount;
	}

	public void register(final String empl_pk, final Boolean permanent, final EmployeeStatistics stats) {
		this.employeesStatistics.put(empl_pk, stats);
		stats.getCertificates().forEach((cert, stat) -> {
			if (this.certificates.containsKey(cert)) {
				this.certificates.get(cert).register(stat);
			}
		});

		this.employeesCount++;
		if (permanent.booleanValue()) {
			this.permanentsCount++;
		}
	}

	public Map<Integer, SiteCertificateStatistics> getCertificates() {
		return this.certificates;
	}

	public Map<String, EmployeeStatistics> getEmployeesStatistics() {
		return this.employeesStatistics;
	}

	public class SiteCertificateStatistics {

		private int count;
		private final int targetPercentage;
		private final CertificatesRecord certificateInformation;

		public SiteCertificateStatistics(final CertificatesRecord certificateInformation) {
			this.certificateInformation = certificateInformation;
			this.targetPercentage = this.certificateInformation.getValue(CERTIFICATES.CERT_TARGET).intValue();
			this.count = 0;
		}

		protected void register(final EmployeeCertificateStatistics emplCertStat) {
			if (emplCertStat.isValid()) {
				this.count++;
			}
		}

		public int getCount() {
			return this.count;
		}

		public int getTarget() {
			return Double.valueOf(Math.ceil((this.targetPercentage * getEmployeesCount()) / 100.0f)).intValue();
		}

		public int getCountPercentage() {
			return getEmployeesCount() > 0 ? (this.count * 100) / getEmployeesCount() : 0;
		}

		public String getTargetStatus() {
			final int countPercentage = getCountPercentage();
			if (countPercentage >= this.targetPercentage) {
				return "success";
			}

			if (countPercentage >= ((2 * this.targetPercentage) / 3)) {
				return "warning";
			}

			return "danger";
		}
	}
}
