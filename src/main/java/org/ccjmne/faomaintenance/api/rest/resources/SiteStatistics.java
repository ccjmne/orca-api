package org.ccjmne.faomaintenance.api.rest.resources;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ccjmne.faomaintenance.api.rest.resources.EmployeeStatistics.EmployeeCertificateStatistics;

public class SiteStatistics {

	private final Map<Integer, SiteCertificateStatistics> certificates;
	private final Map<String, EmployeeStatistics> employeesStatistics;
	private int employeesCount;
	private int permanentsCount;

	public SiteStatistics(final List<Integer> certificates) {
		this.certificates = new HashMap<>();
		certificates.forEach(cert_pk -> this.certificates.put(cert_pk, new SiteCertificateStatistics()));
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
		stats.getCertificates().forEach((cert, stat) -> this.certificates.computeIfAbsent(cert, unused -> new SiteCertificateStatistics()).register(stat));
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

		public SiteCertificateStatistics() {
			this.count = 0;
		}

		public void register(final EmployeeCertificateStatistics emplCertStat) {
			if (emplCertStat.isValid()) {
				this.count++;
			}
		}

		public int getCount() {
			return this.count;
		}

		public int getRate() {
			return getEmployeesCount() == 0 ? 0 : (this.count * 100) / getEmployeesCount();
		}
	}
}
