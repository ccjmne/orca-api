package org.ccjmne.orca.api.modules;

import static org.ccjmne.orca.jooq.classes.Tables.SITES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAINERPROFILES_TRAININGTYPES;
import static org.ccjmne.orca.jooq.classes.Tables.USERS;
import static org.ccjmne.orca.jooq.classes.Tables.USERS_ROLES;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;

import org.ccjmne.orca.jooq.classes.tables.records.UsersRecord;
import org.ccjmne.orca.jooq.classes.tables.records.UsersRolesRecord;
import org.ccjmne.orca.api.utils.Constants;
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

	public static Restrictions forUser(final String user_id, final DSLContext ctx) {
		return new Restrictions(user_id, ctx);
	}

	private Restrictions(final String user_id, final DSLContext ctx) {
		this.ctx = ctx;
		final Map<String, UsersRolesRecord> roles = ctx.selectFrom(USERS_ROLES)
				.where(USERS_ROLES.USER_ID.eq(user_id)).fetchMap(USERS_ROLES.USRO_TYPE);
		this.accessTrainings = roles.containsKey(Constants.ROLE_ACCESS)
				&& Constants.ACCESS_LEVEL_TRAININGS.equals(roles.get(Constants.ROLE_ACCESS).getUsroLevel());
		this.accessAllSites = roles.containsKey(Constants.ROLE_ACCESS)
				&& (Constants.ACCESS_LEVEL_ALL_SITES.compareTo(roles.get(Constants.ROLE_ACCESS).getUsroLevel()) <= 0);
		final UsersRecord user = ctx.selectFrom(USERS).where(USERS.USER_ID.eq(user_id)).fetchOne();
		if (roles.containsKey(Constants.ROLE_ADMIN)) {
			this.manageEmployeeNotes = roles.get(Constants.ROLE_ADMIN).getUsroLevel().compareTo(Integer.valueOf(1)) >= 0;
			this.manageSites = roles.get(Constants.ROLE_ADMIN).getUsroLevel().compareTo(Integer.valueOf(2)) >= 0;
			this.manageCertificates = roles.get(Constants.ROLE_ADMIN).getUsroLevel().compareTo(Integer.valueOf(3)) >= 0;
			this.manageUsers = roles.get(Constants.ROLE_ADMIN).getUsroLevel().equals(Integer.valueOf(4));
		} else {
			this.manageEmployeeNotes = false;
			this.manageSites = false;
			this.manageCertificates = false;
			this.manageUsers = false;
		}

		this.accessibleDepartment = getAccessibleDepartment(user, roles.get(Constants.ROLE_ACCESS));
		this.accessibleSites = listAccessibleSites(user, roles.get(Constants.ROLE_ACCESS));
		this.manageableTypes = listManageableTypes(roles.get(Constants.ROLE_TRAINER));
	}

	private Integer getAccessibleDepartment(final UsersRecord user, final UsersRolesRecord role) {
		if ((role == null) || !Constants.ACCESS_LEVEL_DEPARTMENT.equals(role.getUsroLevel())) {
			return null;
		}

		switch (user.getUserType()) {
			case Constants.USERTYPE_EMPLOYEE:
				return this.ctx.selectFrom(SITES)
						.where(SITES.SITE_PK.eq(DSL
								.select(SITES_EMPLOYEES.SIEM_SITE_FK).from(SITES_EMPLOYEES)
								.where(SITES_EMPLOYEES.SIEM_EMPL_FK.eq(user.getUserEmplFk())
										.and(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(Constants.CURRENT_UPDATE))
										.and(SITES_EMPLOYEES.SIEM_SITE_FK.ne(Constants.UNASSIGNED_SITE)))
								.asField()))
						.fetchOne(SITES.SITE_DEPT_FK);
			case Constants.USERTYPE_SITE:
				return this.ctx.selectFrom(SITES).where(SITES.SITE_PK.eq(user.getUserSiteFk())).fetchOne(SITES.SITE_DEPT_FK);
			default:
				// Constants.USERTYPE_DEPARTMENT
				return user.getUserDeptFk();
		}
	}

	private List<String> listAccessibleSites(final UsersRecord user, final UsersRolesRecord role) {
		if ((role == null) || (Constants.ACCESS_LEVEL_ALL_SITES.compareTo(role.getUsroLevel()) <= 0)) {
			return Collections.EMPTY_LIST;
		}

		final String site;
		switch (user.getUserType()) {
			case Constants.USERTYPE_DEPARTMENT:
				return this.ctx.selectFrom(SITES).where(SITES.SITE_DEPT_FK.eq(user.getUserDeptFk())).fetch(SITES.SITE_PK);
			case Constants.USERTYPE_EMPLOYEE:
				site = this.ctx.selectFrom(SITES_EMPLOYEES)
						.where(SITES_EMPLOYEES.SIEM_EMPL_FK.eq(user.getUserEmplFk())
								.and(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(Constants.CURRENT_UPDATE))
								.and(SITES_EMPLOYEES.SIEM_SITE_FK.ne(Constants.UNASSIGNED_SITE)))
						.fetchOne(SITES_EMPLOYEES.SIEM_SITE_FK);
				break;
			default:
				// Constants.USERTYPE_SITE
				site = user.getUserSiteFk();
		}

		if (Constants.ACCESS_LEVEL_DEPARTMENT.equals(role.getUsroLevel())) {
			return this.ctx.selectFrom(SITES)
					.where(SITES.SITE_DEPT_FK.eq(DSL.select(SITES.SITE_DEPT_FK).from(SITES).where(SITES.SITE_PK.eq(site))))
					.fetch(SITES.SITE_PK);
		}

		return Collections.singletonList(site);
	}

	private List<Integer> listManageableTypes(final UsersRolesRecord role) {
		if (role == null) {
			return Collections.EMPTY_LIST;
		}

		return this.ctx.selectFrom(TRAINERPROFILES_TRAININGTYPES)
				.where(TRAINERPROFILES_TRAININGTYPES.TPTT_TRPR_FK.eq(role.getUsroTrprFk()))
				.fetch(TRAINERPROFILES_TRAININGTYPES.TPTT_TRTY_FK);
	}

	public boolean canAccessDepartment(final Integer dept_pk) {
		return this.accessAllSites || ((this.accessibleDepartment != null) && this.accessibleDepartment.equals(dept_pk));
	}

	public boolean canAccessSite(final String site_pk) {
		return this.accessAllSites || (this.accessibleSites.contains(site_pk));
	}

	/**
	 * Queries the database every time it's used. Let's try to not use it and
	 * see how it goes.
	 */
	@Deprecated
	// TODO: remove?
	public boolean canAccessEmployee(final String empl_pk) {
		return this.accessAllSites || this.ctx.fetchExists(SITES_EMPLOYEES, SITES_EMPLOYEES.SIEM_EMPL_FK.eq(empl_pk)
				.and(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(Constants.CURRENT_UPDATE))
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
	 * Same implementation as {@link Restrictions#canManageUsers}: the same
	 * authorisation is required.
	 */
	@JsonGetter
	public boolean canManageClient() {
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
