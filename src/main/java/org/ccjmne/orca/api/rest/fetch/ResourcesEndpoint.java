package org.ccjmne.orca.api.rest.fetch;

import static org.ccjmne.orca.jooq.codegen.Tables.CERTIFICATES;
import static org.ccjmne.orca.jooq.codegen.Tables.EMPLOYEES;
import static org.ccjmne.orca.jooq.codegen.Tables.SITES;
import static org.ccjmne.orca.jooq.codegen.Tables.SITES_EMPLOYEES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGS;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGTYPES_CERTIFICATES;
import static org.ccjmne.orca.jooq.codegen.Tables.UPDATES;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import org.ccjmne.orca.api.inject.business.QueryParams;
import org.ccjmne.orca.api.inject.business.RecordsCollator;
import org.ccjmne.orca.api.inject.core.ResourcesSelection;
import org.ccjmne.orca.api.inject.core.StatisticsSelection;
import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.api.utils.Fields;
import org.ccjmne.orca.api.utils.JSONFields;
import org.eclipse.jdt.annotation.NonNull;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JoinType;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Row1;
import org.jooq.SelectQuery;
import org.jooq.Table;
import org.jooq.impl.DSL;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Serves the core resources, whose access is restricted on a per-request
 * level.
 *
 * @author ccjmne
 */
@Path("resources")
public class ResourcesEndpoint {

  @SuppressWarnings("unchecked")
  private static final Table<Record1<String>> OUTCOMES_TABLE = DSL
      .values(Constants.EMPLOYEES_OUTCOMES.stream().map(DSL::row).toArray(Row1[]::new))
      .asTable("employees_outcomes", "outcome");

  private final DSLContext          ctx;
  private final ResourcesSelection  resourcesSelection;
  private final StatisticsSelection statisticsSelection;
  private final RecordsCollator     collator;
  private final QueryParams         parameters;

  @Inject
  public ResourcesEndpoint(
                           final DSLContext ctx,
                           final ResourcesSelection resourcesSelection,
                           final StatisticsSelection statisticsSelection,
                           final RecordsCollator collator,
                           final QueryParams parameters) {
    this.ctx = ctx;
    this.resourcesSelection = resourcesSelection;
    this.statisticsSelection = statisticsSelection;
    this.collator = collator;
    this.parameters = parameters;
  }

  /**
   * EMPLOYEES listing methods
   * ------------------------------------------------------------------------
   *
   * <pre>
   * +--------+------------------------------+-----------+
   * | Method | Path                         | Response  |
   * +--------+------------------------------+-----------+
   * | GET    | /employees                   | PAGINATED |
   * +--------+------------------------------+-----------+
   * | GET    | /employees/{employee}        | SINGLE    |
   * +--------+------------------------------+-----------+
   * | GET    | /sites/{site}/employees      | PAGINATED |
   * +--------+------------------------------+-----------+
   * | GET    | /sessions/{session}/trainees | PAGINATED |
   * +--------+------------------------------+-----------+
   * </pre>
   */

  @GET
  @Path("employees")
  public Record listEmployees() {
    return this.ctx.fetchOne(this.collator.applyPagination(this.findEmployees()));
  }

  @GET
  @Path("employees/{employee}")
  public Record lookupEmployee() {
    return this.ctx.fetchSingle(this.findEmployees());
  }

  @GET
  @Path("sites/{site}/employees")
  public Record listSiteEmployees() {
    return this.listEmployees();
  }

  @GET
  @Path("sessions/{session}/trainees")
  public Record listTrainees() {
    return this.listEmployees();
  }

  /**
   * SITES listing methods
   * ------------------------------------------------------------------------
   *
   * <pre>
   * +--------+---------------+-----------+
   * | Method | Path          | Response  |
   * +--------+---------------+-----------+
   * | GET    | /sites        | PAGINATED |
   * +--------+---------------+-----------+
   * | GET    | /sites/{site} | SINGLE    |
   * +--------+---------------+-----------+
   * </pre>
   */

  @GET
  @Path("sites")
  public Record listSites() {
    return this.ctx.fetchOne(this.collator.applyPagination(this.findSites()));
  }

  @GET
  @Path("sites/{site}")
  public Record lookupSite() {
    return this.ctx.fetchSingle(this.findSites());
  }

  /**
   * SITES-GROUPS listing methods
   * ------------------------------------------------------------------------
   *
   * <pre>
   * +--------+--------------------------+-----------+
   * | Method | Path                     | Response  |
   * +--------+--------------------------+-----------+
   * | GET    | /sites-groups            | PAGINATED |
   * +--------+--------------------------+-----------+
   * | GET    | /sites-groups/{group-by} | PAGINATED |
   * +--------+--------------------------+-----------+
   * </pre>
   */

  @GET
  @Path("sites-groups")
  public Record listSitesGroups() {
    return this.ctx.fetchOne(this.collator.applyPagination(this.findSitesGroups()));
  }

  @GET
  @Path("sites-groups/{group-by}")
  public Record listSitesGroupedBy() {
    return this.listSitesGroups();
  }

  /**
   * SESSIONS listing methods
   * ------------------------------------------------------------------------
   *
   * <pre>
   * +--------+--------------------------------+-----------+
   * | Method | Path                           | Response  |
   * +--------+--------------------------------+-----------+
   * | GET    | /sessions                      | PAGINATED |
   * +--------+--------------------------------+-----------+
   * | GET    | /sessions/{session}            | SINGLE    |
   * +--------+--------------------------------+-----------+
   * | GET    | /employees/{employee}/sessions | PAGINATED |
   * +--------+--------------------------------+-----------+
   * </pre>
   */

  @GET
  @Path("sessions")
  public Record listSessions() {
    return this.ctx.fetchOne(this.collator.applyPagination(this.findSessions()));
  }

  @GET
  @Path("sessions/{session}")
  public Record lookupSession() {
    return this.ctx.fetchSingle(this.findSessions());
  }

  @GET
  @Path("employees/{employee}/sessions")
  public Record lookupEmployeeSessions() {
    return this.listSessions();
  }

  /**
   * UPDATES listing methods
   * TODO: Rewrite when implementing updates administration module
   * ------------------------------------------------------------------------
   *
   * <pre>
   * +--------+-----------------+-----------+
   * | Method | Path            | Response  |
   * +--------+-----------------+-----------+
   * | GET    | /updates        | PAGINATED |
   * +--------+-----------------+-----------+
   * | GET    | /updates/latest | SINGLE    |
   * +--------+-----------------+-----------+
   * | GET    | /updates/{date} | SINGLE    |
   * +--------+-----------------+-----------+
   * </pre>
   */

  @GET
  @Path("updates")
  public Record listUpdates() {
    return this.ctx.fetchOne(this.collator.applyPagination(DSL.selectFrom(UPDATES).orderBy(UPDATES.UPDT_DATE.desc())));
  }

  @GET
  @Path("updates/latest")
  public Record lookupLatestUpdate() {
    return this.lookupUpdate();
  }

  @GET
  @Path("updates/{date}")
  public Record lookupUpdate() {
    return this.ctx.selectFrom(UPDATES).where(UPDATES.UPDT_PK.eq(Fields.selectUpdate(this.parameters.get(QueryParams.DATE)))).fetchAny();
  }

  /**
   * Actual implementations
   * ------------------------------------------------------------------------
   */

  private SelectQuery<Record> findEmployees() {
    final Table<? extends Record> employees = this.resourcesSelection.selectEmployees().asTable();
    final Table<? extends Record> stats = this.statisticsSelection.selectEmployeesStats().asTable();

    return this.collator.applyFAndS(DSL
        .select(employees.fields())
        .select(JSONFields
            .objectAgg(stats.field(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK), stats.fields(Fields.EMPLOYEES_STATS_FIELDS))
            .as("empl_stats"))
        .from(employees)
        .leftOuterJoin(stats).on(stats.field(TRAININGS_EMPLOYEES.TREM_EMPL_FK).eq(employees.field(EMPLOYEES.EMPL_PK)))
        .groupBy(employees.fields()));
  }

  private SelectQuery<Record> findSites() {
    final Table<? extends Record> sites = this.resourcesSelection.selectSites().asTable();
    final Table<? extends Record> stats = this.statisticsSelection.selectSitesStats().asTable();

    return this.collator.applyFAndS(DSL
        .select(sites.fields())
        .select(JSONFields
            .objectAgg(stats.field(CERTIFICATES.CERT_PK), stats.fields(Fields.SITES_STATS_FIELDS))
            .as("site_stats"))
        .from(sites)
        .leftOuterJoin(stats).on(stats.field(SITES_EMPLOYEES.SIEM_SITE_FK).eq(sites.field(SITES.SITE_PK)))
        .groupBy(sites.fields()));
  }

  private SelectQuery<Record> findSitesGroups() {
    final Table<? extends Record> sites = this.resourcesSelection.selectSites().asTable();
    final Field<JsonNode> groupID = this.parameters.get(QueryParams.GROUP_BY_FIELD).as("sgrp_value");
    final Table<? extends Record> groups = DSL
        .select(DSL.sum(sites.field("site_employees_count", Integer.class)).as("sgrp_employees_count"))
        .select(DSL.sum(sites.field("site_permanent_count", Integer.class)).as("sgrp_permanent_count"))
        .select(DSL.count(sites.field(SITES.SITE_PK)).as("sgrp_sites_count"))
        .select(groupID)
        .from(sites)
        .groupBy(groupID).asTable();

    try (final SelectQuery<? extends Record> stats = this.statisticsSelection.selectSitesGroupsStats()) {
      stats.addSelect(groupID);
      stats.addJoin(sites, JoinType.RIGHT_OUTER_JOIN, sites.field(SITES.SITE_PK).eq(Fields.unqualify(SITES_EMPLOYEES.SIEM_SITE_FK)));
      stats.addGroupBy(groupID);

      return this.collator.applyFAndS(DSL
          .select(groups.fields())
          .select(JSONFields.objectAgg(stats.field(CERTIFICATES.CERT_PK), stats
              .fields(Fields.SITES_GROUPS_STATS_FIELDS))
              .as("sgrp_stats"))
          .from(groups)
          .leftOuterJoin(stats).on(stats.field(groupID).eq(groups.field(groupID)).or(stats.field(groupID).isNull().and(groups.field(groupID).isNull())))
          .groupBy(groups.fields()));
    }
  }

  private SelectQuery<Record> findSessions() {
    final Table<Record> sessions = this.resourcesSelection.selectSessions().asTable();
    final Table<? extends Record> stats = this.statisticsSelection.selectSessionsStats().asTable();

    final Field<@NonNull String> outcome = OUTCOMES_TABLE.field(0, String.class);
    return this.collator.applyFAndS(DSL
        .select(sessions.fields())
        .select(JSONFields.objectAgg(outcome, DSL.coalesce(stats.field("count"), DSL.zero())).as("stats"))
        .select(DSL.sum(stats.field("count", Integer.class)).as("trainees_count"))
        .from(sessions)
        .join(OUTCOMES_TABLE).on(DSL.noCondition())
        .leftOuterJoin(stats)
        .on(stats.field(TRAININGS_EMPLOYEES.TREM_TRNG_FK).eq(sessions.field(TRAININGS.TRNG_PK)))
        .and(stats.field(TRAININGS_EMPLOYEES.TREM_OUTCOME).eq(outcome))
        .groupBy(sessions.fields()));
  }
}
