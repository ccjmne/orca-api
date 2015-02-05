package org.ccjmne.faomaintenance.api.rest;

import java.util.HashMap;
import java.util.Map;

import org.ccjmne.faomaintenance.api.rest.resources.EmployeeStatistics;
import org.ccjmne.faomaintenance.api.rest.resources.EmployeeStatistics.EmployeeCertificateStatistics;

import com.fasterxml.jackson.annotation.JsonAnyGetter;

public class SiteStatistics {

	private final Map<Integer, SiteCertificateStatistics> statistics;

	public SiteStatistics() {
		this.statistics = new HashMap<>();
	}

	public void register(final EmployeeStatistics employeeStatistics) {
		employeeStatistics.getStatistics().forEach(
													(cert, stat) -> this.statistics.computeIfAbsent(cert, unused -> new SiteCertificateStatistics())
															.register(stat));
	}

	@JsonAnyGetter
	public Map<Integer, SiteCertificateStatistics> getStatistics() {
		return this.statistics;
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
	}
}
