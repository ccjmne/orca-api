package org.ccjmne.faomaintenance.api.rest.resources;

import java.util.HashMap;
import java.util.Map;

import org.ccjmne.faomaintenance.api.rest.resources.EmployeeStatistics.EmployeeCertificateStatistics;

import com.fasterxml.jackson.annotation.JsonAnyGetter;

public class SiteStatistics {

	private final Map<Integer, SiteCertificateStatistics> statistics;
	private int employeesCount;
	private int permanentsCount;

	public SiteStatistics() {
		this.statistics = new HashMap<>();
		this.employeesCount = 0;
		this.permanentsCount = 0;
	}

	public int getEmployeesCount() {
		return this.employeesCount;
	}

	public int getPermanentsCount() {
		return this.permanentsCount;
	}

	public void register(final Boolean permanent, final EmployeeStatistics stats) {
		stats.getStatistics().forEach((cert, stat) -> this.statistics.computeIfAbsent(cert, unused -> new SiteCertificateStatistics()).register(stat));
		this.employeesCount++;
		if (permanent.booleanValue()) {
			this.permanentsCount++;
		}
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
