package org.ccjmne.faomaintenance.api.resources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ccjmne.faomaintenance.api.resources.Employee.EmployeeTrainingsStatistics;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class Site {

	public String aurore;
	public String name;

	public Coordinates location;

	@JsonIgnore
	public List<Employee> employees;
	private Map<String, SiteTrainingsStatistics> statistics;

	public long getPermanentEmployeesCount() {
		return this.employees.stream().filter(e -> e.isPermanent()).count();
	}

	public int getEmployeesCount() {
		return this.employees.size();
	}

	public Site() {
		this.employees = new ArrayList<>();
	}

	public Map<String, SiteTrainingsStatistics> getStatistics() {
		if (this.statistics == null) {
			this.statistics = new HashMap<>();
			this.employees.forEach(e -> e.getStatistics().forEach(
																	(c, s) -> this.statistics
																			.compute(c, (cert, stats) -> (stats == null) ? new SiteTrainingsStatistics(s)
																														: stats.register(s))));
		}
		return this.statistics;
	}

	@SuppressWarnings("boxing")
	public Map<String, Integer> getPercentiles() {
		final Map<String, Integer> percentiles = new HashMap<>();
		getStatistics().forEach((type, sts) -> percentiles.put(type, (sts.currentlyValidCount * 100) / getEmployeesCount()));
		return percentiles;
	}

	public static class SiteTrainingsStatistics {

		public int currentlyValidCount;

		public SiteTrainingsStatistics() {
			this.currentlyValidCount = 0;
		}

		public SiteTrainingsStatistics(final EmployeeTrainingsStatistics employeeTrainingsStatistics) {
			register(employeeTrainingsStatistics);
		}

		public final SiteTrainingsStatistics register(final EmployeeTrainingsStatistics employeeTrainingsStatistics) {
			this.currentlyValidCount += (employeeTrainingsStatistics.isCurrentlyValid()) ? 1 : 0;
			return this;
		}
	}

	public class Coordinates {

		public Double lat;
		public Double lng;
	}
}
