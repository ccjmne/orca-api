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
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import org.ccjmne.orca.api.modules.QueryParameters;
import org.ccjmne.orca.api.modules.RecordsCollator;
import org.ccjmne.orca.api.modules.Restrictions;
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
 * Serves the resources whose access is restricted based on the request's
 * associated {@link Restrictions}.<br />
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

  // TODO: Should not have any use for this and should delegate restricted
  // data access mechanics to RestrictedResourcesHelper
  private final Restrictions restrictions;

  @Inject
  public ResourcesEndpoint(
                           final DSLContext ctx,
                           final Restrictions restrictions,
                           final ResourcesSelection resourcesSelection,
                           final StatisticsSelection statisticsSelection,
                           final RecordsCollator collator,
                           final QueryParameters parameters) {
    this.ctx = ctx;
    this.restrictions = restrictions;
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

  /*
   * EMPLOYEES listing methods
   * ------------------------------------------------------------------------
   * GET /employees
   * GET /employees/{employee}
   * GET /sites/{site}/employees
   * GET /sessions/{session}/trainees
   */

  @GET
  @Path("employees")
  public Result<? extends Record> listEmployees() {
    final Table<? extends Record> employees = this.resourcesSelection.selectEmployees().asTable();
    final Table<? extends Record> stats = this.statisticsSelection.selectEmployeesStats().asTable();
    return this.ctx.fetch(this.collator.applyAll(DSL
        .select(employees.fields())
        .select(ResourcesHelper
            .jsonbObjectAggNullSafe(stats.field(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK), stats.fields("status", "expiry", "void_since"))
            .as("empl_stats"))
        .from(employees)
        .leftOuterJoin(stats).on(stats.field(TRAININGS_EMPLOYEES.TREM_EMPL_FK).eq(employees.field(EMPLOYEES.EMPL_PK)))
        .groupBy(employees.fields()).getQuery()));
  }

  @GET
  @Path("employees/{employee}")
  public Record lookupEmployee() {
    try {
      return this.listEmployees().get(0);
    } catch (final IndexOutOfBoundsException e) {
      // TODO: Maybe handle with an ExceptionMapper
      throw new NotFoundException();
    }
  }

  @GET
  @Path("sites/{site}/employees")
  public Result<? extends Record> listSiteEmployees() {
    return this.listEmployees();
  }

  @GET
  @Path("sessions/{session}/trainees")
  public Result<? extends Record> listTrainees() {
    return this.listEmployees();
  }

  /*
   * SITES listing methods
   * ------------------------------------------------------------------------
   * GET /sites
   * GET /sites/{site}
   */

  @GET
  @Path("sites")
  public Result<? extends Record> listSites() {
    final Table<? extends Record> sites = this.resourcesSelection.selectSites().asTable();
    final Table<? extends Record> stats = this.statisticsSelection.selectSitesStats().asTable();
    return this.ctx.fetch(this.collator.applyAll(DSL
        .select(sites.fields())
        .select(ResourcesHelper.jsonbObjectAggNullSafe(stats.field(CERTIFICATES.CERT_PK),
                                                       stats.fields("status", "current", "target", "success", "warning", "danger"))
            .as("site_stats"))
        .from(sites)
        .leftOuterJoin(stats).on(stats.field(SITES_EMPLOYEES.SIEM_SITE_FK).eq(sites.field(SITES.SITE_PK)))
        .groupBy(sites.fields())));
  }

  @GET
  @Path("sites/{site}")
  public Record lookupSite() {
    try {
      return this.listSites().get(0);
    } catch (final IndexOutOfBoundsException e) {
      // TODO: Maybe handle with an ExceptionMapper
      throw new NotFoundException();
    }
  }

  /*
   * SITES-GROUPS listing methods
   * ------------------------------------------------------------------------
   * GET /sites-groups
   * GET /sites-groups/{group-by}
   */

  @GET
  @Path("sites-groups")
  public Result<? extends Record> listSitesGroups() {
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
      stats.addJoin(sites,
                    JoinType.RIGHT_OUTER_JOIN,
                    sites.field(SITES.SITE_PK).eq(Constants.unqualify(SITES_EMPLOYEES.SIEM_SITE_FK)));
      stats.addGroupBy(groupID);

      return this.ctx.fetch(this.collator.applyAll(DSL
          .select(groups.fields())
          .select(ResourcesHelper.jsonbObjectAggNullSafe(stats.field(CERTIFICATES.CERT_PK), stats
              .fields("status", "current", "target", "score", Constants.STATUS_SUCCESS, Constants.STATUS_WARNING, Constants.STATUS_DANGER,
                      "sites_" + Constants.STATUS_SUCCESS, "sites_" + Constants.STATUS_WARNING, "sites_" + Constants.STATUS_DANGER))
              .as("sgrp_stats"))
          .from(groups)
          .leftOuterJoin(stats).on(stats.field(groupID).eq(groups.field(groupID))
              .or(stats.field(groupID).isNull().and(groups.field(groupID).isNull())))
          .groupBy(groups.fields())));
    }
  }

  @GET
  @Path("sites-groups/{group-by}")
  public Result<? extends Record> listSitesGroupedBy() {
    return this.listSitesGroups();
  }

  /*
   * SESSIONS listing methods
   * ------------------------------------------------------------------------
   * GET /sessions
   * GET /sessions/{session}
   * GET /employees/{employee}/sessions
   */

  @GET
  @Path("sessions")
  public Result<Record> listSessions() {
    final Table<Record> sessions = this.resourcesSelection.selectSessions().asTable();
    final Table<? extends Record> stats = StatisticsSelection.selectSessionsStats().asTable();

    return this.ctx.fetch(this.collator.applyAll(DSL.select(sessions.fields())
        .select(ResourcesHelper.jsonbObjectAggNullSafe(stats.field("outcome"), stats.field("count")).as("stats"))
        .select(DSL.sum(stats.field("count", Integer.class)).as("trainees_count"))
        .from(sessions)
        .leftOuterJoin(stats)
        .on(stats.field(TRAININGS_EMPLOYEES.TREM_TRNG_FK).eq(sessions.field(TRAININGS.TRNG_PK)))
        .groupBy(sessions.fields())));
  }

  @GET
  @Path("sessions/{session}")
  public Record lookupSession() {
    try {
      return this.listSessions().get(0);
    } catch (final IndexOutOfBoundsException e) {
      // TODO: Maybe handle with an ExceptionMapper
      throw new NotFoundException();
    }
  }

  @GET
  @Path("employees/{employee}/sessions")
  public Result<Record> lookupEmployeeSessions() {
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
  public Record lookupUpdate(@PathParam("date") final String dateStr) {
    return this.ctx.selectFrom(UPDATES).where(UPDATES.UPDT_PK.eq(Constants.selectUpdate(dateStr))).fetchAny();
  }
}
