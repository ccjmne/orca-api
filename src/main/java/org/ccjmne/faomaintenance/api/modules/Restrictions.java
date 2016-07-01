package org.ccjmne.faomaintenance.api.modules;

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

import org.ccjmne.faomaintenance.api.utils.Constants;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.EmployeesRolesRecord;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import com.fasterxml.jackson.annotation.JsonGetter;

/**
 * Provides tools to manage restrictions of resources access or manipulation
 * provided an authenticated {@link HttpServletRequest} or a specific user ID.
 */
public class Restrictions {
	private final DSLContext ctx;

	private final boolean accessTrainings;
	private final boolean accessAllSites;
	private final boolean manageEmployeeNotes;
	private final boolean manageSites;
	private final boolean manageCertificates;
	private final boolean manageUsers;
	private final List<String> accessibleSites;
	private final List<Integer> manageableTypes;
	private final Integer accessibleDepartment;

	@Inject
	public Restrictions(@Context final HttpServletRequest request, final DSLContext ctx) {
		this(request.getRemoteUser(), ctx);
	}

	public static Restrictions forEmployee(final String empl_pk, final DSLContext ctx) {
		return new Restrictions(empl_pk, ctx);
	}

	private Restrictions(final String empl_pk, final DSLContext ctx) {
		this.ctx = ctx;
		final Map<String, EmployeesRolesRecord> roles = ctx.selectFrom(EMPLOYEES_ROLES)
				.where(EMPLOYEES_ROLES.EMPL_PK.eq(empl_pk)).fetchMap(EMPLOYEES_ROLES.EMRO_TYPE);
		this.accessTrainings = roles.containsKey(Constants.ROLE_ACCESS)
				&& Constants.ACCESS_LEVEL_TRAININGS.equals(roles.get(Constants.ROLE_ACCESS).getEmroLevel());
		this.accessAllSites = roles.containsKey(Constants.ROLE_ACCESS)
				&& (Constants.ACCESS_LEVEL_ALL_SITES.compareTo(roles.get(Constants.ROLE_ACCESS).getEmroLevel()) <= 0);
		if (roles.containsKey(Constants.ROLE_ADMIN)) {
			this.manageEmployeeNotes = roles.get(Constants.ROLE_ADMIN).getEmroLevel().compareTo(Integer.valueOf(1)) >= 0;
			this.manageSites = roles.get(Constants.ROLE_ADMIN).getEmroLevel().compareTo(Integer.valueOf(2)) >= 0;
			this.manageCertificates = roles.get(Constants.ROLE_ADMIN).getEmroLevel().compareTo(Integer.valueOf(3)) >= 0;
			this.manageUsers = roles.get(Constants.ROLE_ADMIN).getEmroLevel().equals(Integer.valueOf(4));
		} else {
			this.manageEmployeeNotes = false;
			this.manageSites = false;
			this.manageCertificates = false;
			this.manageUsers = false;
		}
		this.accessibleDepartment = getAccessibleDepartment(empl_pk, roles.get(Constants.ROLE_ACCESS));
		this.accessibleSites = listAccessibleSites(empl_pk, roles.get(Constants.ROLE_ACCESS));
		this.manageableTypes = listManageableTypes(roles.get(Constants.ROLE_TRAINER));
	}

	private Integer getAccessibleDepartment(final String empl_pk, final EmployeesRolesRecord role) {
		if ((role == null) || !Constants.ACCESS_LEVEL_ONE_DEPT.equals(role.getEmroLevel())) {
			return null;
		}

		return this.ctx.selectFrom(SITES)
				.where(SITES.SITE_PK.eq(DSL
						.select(SITES_EMPLOYEES.SIEM_SITE_FK).from(SITES_EMPLOYEES)
						.where(SITES_EMPLOYEES.SIEM_EMPL_FK.eq(empl_pk)
								.and(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(Constants.LATEST_UPDATE))
								.and(SITES_EMPLOYEES.SIEM_SITE_FK.ne(Constants.UNASSIGNED_SITE)))
						.asField()))
				.fetchOne(SITES.SITE_DEPT_FK);
	}

	private List<Integer> listManageableTypes(final EmployeesRolesRecord role) {
		if (role == null) {
			return Collections.EMPTY_LIST;
		}

		return this.ctx.selectFrom(TRAINERLEVELS_TRAININGTYPES)
				.where(TRAINERLEVELS_TRAININGTYPES.TLTR_TRLV_FK.eq(role.getEmroTrlvFk()))
				.fetch(TRAINERLEVELS_TRAININGTYPES.TLTR_TRTY_FK);
	}

	private List<String> listAccessibleSites(final String empl_pk, final EmployeesRolesRecord role) {
		if ((role == null) || (Constants.ACCESS_LEVEL_ALL_SITES.compareTo(role.getEmroLevel()) <= 0)) {
			return Collections.EMPTY_LIST;
		}

		final String site = this.ctx.selectFrom(SITES_EMPLOYEES)
				.where(SITES_EMPLOYEES.SIEM_EMPL_FK.eq(empl_pk)
						.and(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(Constants.LATEST_UPDATE))
						.and(SITES_EMPLOYEES.SIEM_SITE_FK.ne(Constants.UNASSIGNED_SITE)))
				.fetchOne(SITES_EMPLOYEES.SIEM_SITE_FK);

		if (site == null) {
			return Collections.EMPTY_LIST;
		}

		if (Constants.ACCESS_LEVEL_ONE_DEPT.equals(role.getEmroLevel())) {
			return this.ctx.selectFrom(SITES)
					.where(SITES.SITE_DEPT_FK.eq(DSL.select(SITES.SITE_DEPT_FK).from(SITES).where(SITES.SITE_PK.eq(site))))
					.fetch(SITES.SITE_PK);
		}

		return Collections.singletonList(site);
	}

	public boolean canAccessDepartment(final Integer dept_pk) {
		return this.accessAllSites || ((this.accessibleDepartment != null) && this.accessibleDepartment.equals(dept_pk));
	}

	public boolean canAccessEmployee(final String empl_pk) {
		return this.accessAllSites || this.ctx.fetchExists(SITES_EMPLOYEES, SITES_EMPLOYEES.SIEM_EMPL_FK.eq(empl_pk)
				.and(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(Constants.LATEST_UPDATE))
				.and(SITES_EMPLOYEES.SIEM_SITE_FK.in(this.accessibleSites)));
	}

	@JsonGetter
	public boolean canAccessTrainings() {
		return this.accessTrainings;
	}

	/**
	 * Checks whether the current {@link HttpServletRequest} can access all
	 * existing sites.<br />
	 * Should this method return <code>true</code>, both
	 * {@link Restrictions#getAccessibleDepartment()} and
	 * {@link Restrictions#getAccessibleSites()} return <code>null</code> and an
	 * empty <code>List</code> respectively.
	 *
	 * @return <code>true</code> if all sites are accessible to the injected
	 *         request.
	 */
	@JsonGetter
	public boolean canAccessAllSites() {
		return this.accessAllSites;
	}

	@JsonGetter
	public boolean canManageEmployeeNotes() {
		return this.manageEmployeeNotes;
	}

	@JsonGetter
	public boolean canManageSites() {
		return this.manageSites;
	}

	@JsonGetter
	public boolean canManageCertificates() {
		return this.manageCertificates;
	}

	@JsonGetter
	public boolean canManageUsers() {
		return this.manageUsers;
	}

	/**
	 * Get the department the current {@link HttpServletRequest}'s scope should
	 * be restricted to, if relevant.
	 *
	 * @return The only department accessible or <code>null</code> if there is
	 *         no such department.
	 * @see {@link Restrictions#canAccessAllSites()}
	 */
	public Integer getAccessibleDepartment() {
		return this.accessibleDepartment;
	}

	/**
	 * Get the sites the current {@link HttpServletRequest}'s scope should be
	 * restricted to or an <b>empty list</b> if all sites are accessible.
	 *
	 * @return The list of accessible sites if relevant.
	 * @see {@link Restrictions#canAccessAllSites()}
	 */
	public List<String> getAccessibleSites() {
		return this.accessibleSites;
	}

	public List<Integer> getManageableTypes() {
		return this.manageableTypes;
	}
}
