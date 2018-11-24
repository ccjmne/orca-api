package org.ccjmne.orca.api.rest.fetch;

import static org.ccjmne.orca.jooq.classes.Tables.CERTIFICATES;
import static org.ccjmne.orca.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGTYPES_CERTIFICATES;
import static org.ccjmne.orca.jooq.classes.Tables.UPDATES;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import org.ccjmne.orca.api.inject.QueryParameters;
import org.ccjmne.orca.api.inject.RecordsCollator;
import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.api.utils.ResourcesHelper;
import org.ccjmne.orca.api.utils.ResourcesSelection;
import org.ccjmne.orca.api.utils.StatisticsSelection;
import org.ccjmne.orca.jooq.classes.tables.records.UpdatesRecord;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JoinType;
import org.jooq.Record;
import org.jooq.Result;
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

  private final DSLContext          ctx;
  private final ResourcesSelection  resourcesSelection;
  private final StatisticsSelection statisticsSelection;
  private final RecordsCollator     collator;
  private final QueryParameters     parameters;

  @Inject
  public ResourcesEndpoint(
                           final DSLContext ctx,
                           final ResourcesSelection resourcesSelection,
                           final StatisticsSelection statisticsSelection,
                           final RecordsCollator collator,
                           final QueryParameters parameters) {
    this.ctx = ctx;
    this.resourcesSelection = resourcesSelection;
    this.statisticsSelection = statisticsSelection;
    this.collator = collator;
    this.parameters = parameters;
  }

  /**
   * TODO: Implement history methods
   *
   * All the <code>/history</code> methods only concern a single
   * <strong>resource</strong>.
   *
   * TODO: Leverage PostgreSQL's <code>generate_series</code> and only query the
   * DB once<br />
   * Code sample:
   *
   * <pre>
   * SELECT generate_series(
   *     date_trunc('month', '2018-01-23'::date),
   *     '2018-05-15'::date,
   *     '1 month'
   * )::date
   * </pre>
   */

  /**
   * TODO: Implement resources ID to "Display Name" listing methods
   *
   * Needs to be a {@code Map} instead of a {@code List}.
   */

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
    return this.ctx.fetchOne(this.findEmployees());
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
    return this.ctx.fetchOne(this.findSites());
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
    return this.ctx.fetchOne(this.collator.applyFAndS(this.findSessions()));
  }

  @GET
  @Path("employees/{employee}/sessions")
  public Record lookupEmployeeSessions() {
    return this.listSessions();
  }

  @GET
  @Path("updates")
  // TODO: move to UpdateEndpoint?
  public Result<UpdatesRecord> listUpdates() {
    return this.ctx.selectFrom(UPDATES).orderBy(UPDATES.UPDT_DATE.desc()).fetch();
  }

  @GET
  @Path("updates/{date}")
  // TODO: move to UpdateEndpoint?
  public Record lookupUpdate() {
    return this.ctx.selectFrom(UPDATES).where(UPDATES.UPDT_PK.eq(Constants.selectUpdate(this.parameters.get(QueryParameters.DATE)))).fetchAny();
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
        .select(ResourcesHelper
            .jsonbObjectAggNullSafe(stats.field(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK), stats.fields("status", "expiry", "void_since"))
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
        .select(ResourcesHelper
            .jsonbObjectAggNullSafe(stats.field(CERTIFICATES.CERT_PK), stats.fields("status", "current", "target", "success", "warning", "danger"))
            .as("site_stats"))
        .from(sites)
        .leftOuterJoin(stats).on(stats.field(SITES_EMPLOYEES.SIEM_SITE_FK).eq(sites.field(SITES.SITE_PK)))
        .groupBy(sites.fields()));
  }

  private SelectQuery<Record> findSitesGroups() {
    final Table<? extends Record> sites = this.resourcesSelection.selectSites().asTable();
    final Field<JsonNode> groupID = this.parameters.get(QueryParameters.GROUP_BY_FIELD).as("sgrp_value");
    final Table<? extends Record> groups = DSL
        .select(DSL.sum(sites.field("site_employees_count", Integer.class)).as("sgrp_employees_count"))
        .select(DSL.sum(sites.field("site_permanent_count", Integer.class)).as("sgrp_permanent_count"))
        .select(DSL.count(sites.field(SITES.SITE_PK)).as("sgrp_sites_count"))
        .select(groupID)
        .from(sites)
        .groupBy(groupID).asTable();

    try (final SelectQuery<? extends Record> stats = this.statisticsSelection.selectSitesGroupsStats()) {
      stats.addSelect(groupID);
      stats.addJoin(sites, JoinType.RIGHT_OUTER_JOIN, sites.field(SITES.SITE_PK).eq(Constants.unqualify(SITES_EMPLOYEES.SIEM_SITE_FK)));
      stats.addGroupBy(groupID);

      return this.collator.applyFAndS(DSL
          .select(groups.fields())
          .select(ResourcesHelper.jsonbObjectAggNullSafe(stats.field(CERTIFICATES.CERT_PK), stats
              .fields("status", "current", "score", "success", "warning", "danger", "sites_success", "sites_warning", "sites_danger"))
              .as("sgrp_stats"))
          .from(groups)
          .leftOuterJoin(stats).on(stats.field(groupID).eq(groups.field(groupID)).or(stats.field(groupID).isNull().and(groups.field(groupID).isNull())))
          .groupBy(groups.fields()));
    }
  }

  private SelectQuery<Record> findSessions() {
    final Table<Record> sessions = this.resourcesSelection.selectSessions().asTable();
    final Table<? extends Record> stats = StatisticsSelection.selectSessionsStats().asTable();

    return this.collator.applyFAndS(DSL
        .select(sessions.fields())
        .select(ResourcesHelper.jsonbObjectAggNullSafe(stats.field("outcome"), stats.field("count")).as("stats"))
        .select(DSL.sum(stats.field("count", Integer.class)).as("trainees_count"))
        .from(sessions)
        .leftOuterJoin(stats)
        .on(stats.field(TRAININGS_EMPLOYEES.TREM_TRNG_FK).eq(sessions.field(TRAININGS.TRNG_PK)))
        .groupBy(sessions.fields()));
  }
}
