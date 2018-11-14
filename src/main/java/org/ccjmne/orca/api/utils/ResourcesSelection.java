package org.ccjmne.orca.api.utils;

import static org.ccjmne.orca.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES_TAGS;
import static org.ccjmne.orca.jooq.classes.Tables.TAGS;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS_EMPLOYEES;

import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;

import org.ccjmne.orca.api.modules.QueryParameters;
import org.ccjmne.orca.api.modules.RecordsCollator;
import org.ccjmne.orca.api.modules.Restrictions;
import org.jooq.JoinType;
import org.jooq.Record;
import org.jooq.SelectQuery;
import org.jooq.impl.DSL;

/**
 * Used to select resources whose access should be restricted.<br />
 * <strong>All access to these resources shall be done through this
 * class!</strong><br />
 *
 * @author ccjmne
 */
public class ResourcesSelection {

	private final QueryParameters parameters;
	private final Restrictions restrictions;
	private final RecordsCollator recordsCollator;

	@Inject
	public ResourcesSelection(final QueryParameters parameters, final Restrictions restrictions, final RecordsCollator recordsCollator) {
		this.parameters = parameters;
		this.restrictions = restrictions;
		this.recordsCollator = recordsCollator;
	}

	/**
	 * "Retired" employees (i.e.: those that aren't assigned to any site as per
	 * the most relevant update) should only ever be accessed through their
	 * training sessions, since they only need to keep existing there for
	 * history purposes.<br />
	 * Thus, employees that aren't assigned to any site can be accessed if and
	 * only if:
	 * <ul>
	 * <li>Fetching a specific employee ({@code has(QueryParameters.EMPLOYEE)}),
	 * and:</li>
	 * <li>{@link Restrictions#canAccessTrainings()} is {@code true}</li>
	 * </ul>
	 * Or:
	 * <ul>
	 * <li>Fetching a specific training session
	 * ({@code has(QueryParameters.SESSION)})</li>
	 * </ul>
	 */
	public boolean includeRetiredEmployees() {
		return this.parameters.has(QueryParameters.SESSION) || (this.parameters.has(QueryParameters.EMPLOYEE) && this.restrictions.canAccessTrainings());
	}

	public SelectQuery<Record> selectEmployees() {
		try (final SelectQuery<Record> query = DSL.select().getQuery()) {
			query.addFrom(EMPLOYEES);
			query.addConditions(EMPLOYEES.EMPL_PK.ne(Constants.EMPLOYEE_ROOT));
			// TODO: Use DSL.noCondition() when upgrading jOOQ
			query.addJoin(
							SITES_EMPLOYEES,
							SITES_EMPLOYEES.SIEM_EMPL_FK.eq(EMPLOYEES.EMPL_PK),
							SITES_EMPLOYEES.SIEM_UPDT_FK.eq(Constants.selectUpdate(this.parameters.of(QueryParameters.DATE))),
							DSL.or(	SITES_EMPLOYEES.SIEM_SITE_FK.in(Constants.select(SITES.SITE_PK, this.selectSites())),
									this.includeRetiredEmployees() ? SITES_EMPLOYEES.SIEM_SITE_FK.isNull() : DSL.falseCondition()));

			if (this.parameters.has(QueryParameters.SESSION)) {
				if (!this.restrictions.canAccessTrainings()) {
					throw new ForbiddenException();
				}

				query.addJoin(	TRAININGS_EMPLOYEES, TRAININGS_EMPLOYEES.TREM_EMPL_FK.eq(EMPLOYEES.EMPL_PK),
								TRAININGS_EMPLOYEES.TREM_TRNG_FK.eq(this.parameters.get(QueryParameters.SESSION)));
			}

			if (this.parameters.has(QueryParameters.EMPLOYEE)) {
				query.addConditions(EMPLOYEES.EMPL_PK.eq(this.parameters.get(QueryParameters.EMPLOYEE)));
			}

			return this.recordsCollator.applyFAndS(query);
		}
	}

	public SelectQuery<Record> selectSites() {
		try (final SelectQuery<Record> query = DSL.select().getQuery()) {
			query.addSelect(SITES.fields());
			query.addFrom(SITES);
			query.addConditions(SITES.SITE_PK.ne(Constants.DECOMMISSIONED_SITE));
			query.addSelect(DSL.count(SITES_EMPLOYEES.SIEM_EMPL_FK).as("site_employees_count"));
			query.addSelect(DSL.count(SITES_EMPLOYEES.SIEM_EMPL_FK).filterWhere(EMPLOYEES.EMPL_PERMANENT.eq(Boolean.TRUE)).as("site_permanent_count"));
			query.addJoin(	SITES_EMPLOYEES.join(EMPLOYEES).on(EMPLOYEES.EMPL_PK.eq(SITES_EMPLOYEES.SIEM_EMPL_FK)),
							this.parameters.is(QueryParameters.INCLUDE_DECOMISSIONED, Boolean.TRUE) ? JoinType.LEFT_OUTER_JOIN : JoinType.JOIN,
							SITES_EMPLOYEES.SIEM_SITE_FK.eq(SITES.SITE_PK));

			if (this.parameters.has(QueryParameters.SITE)) {
				if (!this.restrictions.canAccessSite(this.parameters.getRaw(QueryParameters.SITE))) {
					throw new ForbiddenException();
				}

				query.addConditions(SITES.SITE_PK.eq(this.parameters.get(QueryParameters.SITE)));
			} else if (!this.restrictions.canAccessAllSites()) {
				if (this.restrictions.getAccessibleSites().isEmpty()) {
					throw new ForbiddenException();
				}

				query.addConditions(SITES.SITE_PK.in(this.restrictions.getAccessibleSites()));
			}

			query.addGroupBy(SITES.fields());

			return this.recordsCollator.applyFAndS(DSL
					.select(query.fields())
					.select(ResourcesHelper.jsonbObjectAggNullSafe(SITES_TAGS.SITA_TAGS_FK, ResourcesHelper.TAG_VALUE_COERCED).as("site_tags"))
					.from(query)
					.leftOuterJoin(SITES_TAGS).on(SITES_TAGS.SITA_SITE_FK.eq(query.field(SITES.SITE_PK)))
					.join(TAGS).on(TAGS.TAGS_PK.eq(SITES_TAGS.SITA_TAGS_FK)) // In order to extract TAGS_TYPE for TAG_VALUE_COERCED
					.groupBy(query.fields()));
		}
	}
}
