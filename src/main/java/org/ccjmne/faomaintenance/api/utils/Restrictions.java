package org.ccjmne.faomaintenance.api.utils;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES_ROLES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAINERLEVELS_TRAININGTYPES;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;

import org.ccjmne.faomaintenance.jooq.classes.tables.records.EmployeesRolesRecord;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

public class Restrictions {
	private final boolean accessTrainings;
	private final boolean accessAllSites;
	private final List<String> accessibleSites;
	private final List<Integer> manageableTypes;

	@Inject
	public Restrictions(@Context final HttpServletRequest request, final DSLContext ctx) {
		final Map<String, EmployeesRolesRecord> roles = ctx.selectFrom(EMPLOYEES_ROLES)
				.where(EMPLOYEES_ROLES.EMPL_PK.eq(request.getRemoteUser())).fetchMap(EMPLOYEES_ROLES.EMRO_TYPE);
		this.accessTrainings = Constants.TRAININGS_ACCESS.equals(roles.get(Constants.ROLE_ACCESS).getEmroLevel());
		this.accessAllSites = Constants.ALL_SITES_ACCESS.compareTo(roles.get(Constants.ROLE_ACCESS).getEmroLevel()) <= 0;
		this.accessibleSites = listAccessibleSites(request.getRemoteUser(), roles.get(Constants.ROLE_ACCESS), ctx);
		this.manageableTypes = listManageableTypes(roles.get(Constants.ROLE_TRAINER), ctx);
	}

	private static List<Integer> listManageableTypes(final EmployeesRolesRecord role, final DSLContext ctx) {
		if (role == null) {
			return Collections.EMPTY_LIST;
		}

		return ctx.selectFrom(TRAINERLEVELS_TRAININGTYPES)
				.where(TRAINERLEVELS_TRAININGTYPES.TLTR_TRLV_FK.eq(role.getEmroTrlvFk()))
				.fetch(TRAINERLEVELS_TRAININGTYPES.TLTR_TRTY_FK);
	}

	public List<String> initSites(final String site) {
		return Collections.EMPTY_LIST;
	}

	public static List<String> listAccessibleSites(final String empl_pk, final EmployeesRolesRecord role, final DSLContext ctx) {
		if ((role == null) || (Constants.ALL_SITES_ACCESS.compareTo(role.getEmroLevel()) <= 0)) {
			return Collections.EMPTY_LIST;
		}

		final String site = ctx.select(SITES_EMPLOYEES.SIEM_SITE_FK)
				.from(SITES_EMPLOYEES)
				.where(SITES_EMPLOYEES.SIEM_EMPL_FK.eq(empl_pk)
						.and(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(Constants.LATEST_UPDATE))
						.and(SITES_EMPLOYEES.SIEM_SITE_FK.ne(Constants.SITE_UNASSIGNED)))
				.fetchOne(SITES_EMPLOYEES.SIEM_SITE_FK);

		if (site == null) {
			return Collections.EMPTY_LIST;
		}

		if (Constants.ONLY_DEPT_ACCESS.equals(role.getEmroLevel())) {
			return ctx.select(SITES.SITE_PK)
					.from(SITES)
					.where(SITES.SITE_DEPT_FK.eq(DSL.select(SITES.SITE_DEPT_FK).from(SITES).where(SITES.SITE_PK.eq(site))))
					.fetch(SITES.SITE_PK);
		}

		return Collections.singletonList(site);
	}

	public boolean isAccessTrainings() {
		return this.accessTrainings;
	}

	public boolean isAccessAllSites() {
		return this.accessAllSites;
	}

	public List<String> getAccessibleSites() {
		return this.accessibleSites;
	}

	public List<Integer> getManageableTypes() {
		return this.manageableTypes;
	}
}
