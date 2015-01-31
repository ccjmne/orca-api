package org.ccjmne.faomaintenance.api.resources;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Update {

	private final Date date;
	private final Map<Employee, Site> assignmentMap;

	public Update() {
		this.date = new Date();
		this.assignmentMap = new HashMap<>();
	}

	public void assign(final Employee employee, final Site site) {
		this.assignmentMap.put(employee, site);
	}

	public Date getDate() {
		return this.date;
	}

	public Map<Employee, Site> getAssignmentMap() {
		return this.assignmentMap;
	}
}
