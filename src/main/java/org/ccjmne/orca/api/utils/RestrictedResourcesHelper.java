package org.ccjmne.orca.api.utils;

import static org.ccjmne.orca.jooq.classes.Tables.DEPARTMENTS;
import static org.ccjmne.orca.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS_EMPLOYEES;

import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;

import org.ccjmne.orca.api.modules.Restrictions;
import org.jooq.JoinType;
import org.jooq.Record;
import org.jooq.SelectQuery;
import org.jooq.impl.DSL;

public class RestrictedResourcesHelper {

	private final Restrictions restrictions;

	@Inject
	public RestrictedResourcesHelper(final Restrictions restrictions) {
		this.restrictions = restrictions;
	}

	/**
	 * Unassigned employees should only ever be accessed through their training
	 * sessions, since they only need to keep existing there for history
	 * purposes.<br />
	 * Thus, employees that aren't assigned to any site can be accessed if and
	 * only if:
	 * <ul>
	 * <li><code>dept_pk</code> is <code>null</code></li>
	 * <li><code>site_pk</code> is <code>null</code></li>
	 * <li><code>trng_pk</code> is <strong>defined</strong></li>
	 * </ul>
	 * Or:
	 * <ul>
	 * <li><code>empl_pk</code> is <strong>defined</strong></li>
	 * <li>{@link Restrictions#canAccessTrainings()} is <code>true</code></li>
	 * </ul>
	 */
	private boolean accessUnassignedEmployees(final String empl_pk, final String site_pk, final Integer dept_pk, final Integer trng_pk) {
		if ((empl_pk != null) && this.restrictions.canAccessTrainings()) {
			return true;
		}

		return (dept_pk == null) && (site_pk == null) && (trng_pk != null);
	}

	public SelectQuery<Record> selectEmployees(final String empl_pk, final String site_pk, final Integer dept_pk, final Integer trng_pk, final String dateStr) {
		final SelectQuery<Record> query = DSL.select().getQuery();
		query.addFrom(EMPLOYEES);
		query.addConditions(EMPLOYEES.EMPL_PK.ne(Constants.USER_ROOT));
		query.addJoin(
						SITES_EMPLOYEES,
						accessUnassignedEmployees(empl_pk, site_pk, dept_pk, trng_pk) ? JoinType.LEFT_OUTER_JOIN : JoinType.JOIN,
						SITES_EMPLOYEES.SIEM_EMPL_FK.eq(EMPLOYEES.EMPL_PK),
						SITES_EMPLOYEES.SIEM_UPDT_FK.eq(Constants.selectUpdate(dateStr)),
						SITES_EMPLOYEES.SIEM_SITE_FK.in(Constants.select(SITES.SITE_PK, selectSites(site_pk, dept_pk))));

		if (trng_pk != null) {
			if (!this.restrictions.canAccessTrainings()) {
				throw new ForbiddenException();
			}

			query.addJoin(TRAININGS_EMPLOYEES, TRAININGS_EMPLOYEES.TREM_EMPL_FK.eq(EMPLOYEES.EMPL_PK), TRAININGS_EMPLOYEES.TREM_TRNG_FK.eq(trng_pk));
		}

		if (empl_pk != null) {
			query.addConditions(EMPLOYEES.EMPL_PK.eq(empl_pk));
		}

		return query;
	}

	public SelectQuery<Record> selectSites(final String site_pk, final Integer dept_pk) {
		final SelectQuery<Record> query = DSL.select().getQuery();
		query.addFrom(SITES);
		query.addConditions(SITES.SITE_PK.ne(Constants.UNASSIGNED_SITE));
		if ((site_pk == null) && !this.restrictions.canAccessAllSites()) {
			if (this.restrictions.getAccessibleSites().isEmpty()) {
				throw new ForbiddenException();
			}

			query.addConditions(SITES.SITE_PK.in(this.restrictions.getAccessibleSites()));
		}

		if (dept_pk != null) {
			if (!this.restrictions.canAccessDepartment(dept_pk)) {
				throw new ForbiddenException();
			}

			query.addConditions(SITES.SITE_DEPT_FK.eq(dept_pk));
		}

		if (site_pk != null) {
			if (!this.restrictions.canAccessSite(site_pk)) {
				throw new ForbiddenException();
			}

			query.addConditions(SITES.SITE_PK.eq(site_pk));
		}

		return query;
	}

	public SelectQuery<Record> selectDepartments(final Integer dept_pk) {
		final SelectQuery<Record> query = DSL.select().getQuery();
		query.addFrom(DEPARTMENTS);
		query.addConditions(DEPARTMENTS.DEPT_PK.ne(Constants.UNASSIGNED_DEPARTMENT));
		if ((dept_pk == null) && !this.restrictions.canAccessAllSites()) {
			if (this.restrictions.getAccessibleDepartment() == null) {
				throw new ForbiddenException();
			}

			query.addConditions(DEPARTMENTS.DEPT_PK.eq(this.restrictions.getAccessibleDepartment()));
		}

		if (dept_pk != null) {
			if (!this.restrictions.canAccessDepartment(dept_pk)) {
				throw new ForbiddenException();
			}

			query.addConditions(DEPARTMENTS.DEPT_PK.eq(dept_pk));
		}

		return query;
	}
}
