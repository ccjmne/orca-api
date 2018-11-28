package org.ccjmne.orca.api.rest.fetch;

import static org.ccjmne.orca.jooq.classes.Tables.CERTIFICATES;
import static org.ccjmne.orca.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS_EMPLOYEES;

import java.sql.Date;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import org.ccjmne.orca.api.inject.QueryParameters;
import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.api.utils.ResourcesHelper;
import org.ccjmne.orca.api.utils.ResourcesSelection;
import org.ccjmne.orca.api.utils.StatisticsSelection;
import org.eclipse.jdt.annotation.NonNull;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JoinType;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.SelectQuery;
import org.jooq.Table;
import org.jooq.impl.DSL;

/**
 * Compute statistics for core resources at multiple points in time.
 *
 * @author ccjmne
 */
@Path("statistics-history")
public class StatisticsHistoryEndpoint {

  private static final String DATE_SERIES_FIELD_NAME = "date";

  private final DSLContext          ctx;
  private final QueryParameters     parameters;
  private final ResourcesSelection  resourcesSelection;
  private final StatisticsSelection statisticsSelection;

  private final Field<@NonNull Date> date;

  @Inject
  private StatisticsHistoryEndpoint(final DSLContext ctx, final QueryParameters parameters, final ResourcesSelection resourcesSelection) {
    this.ctx = ctx;
    this.parameters = parameters;
    this.resourcesSelection = resourcesSelection;
    this.statisticsSelection = new StatisticsSelection(DSL.field(DATE_SERIES_FIELD_NAME, Date.class));
    this.date = DSL
        .field("generate_series(date_trunc('month', {0}), {1}, '1 month')::date", Date.class,
               parameters.get(QueryParameters.FROM),
               parameters.get(QueryParameters.TO))
        .as(DATE_SERIES_FIELD_NAME);
  }

  @GET
  @Path("employees/{employee}")
  public Result<? extends Record> getEmployeesStatsHistory() {
    final Table<Record> employees = this.resourcesSelection.selectEmployees().asTable();
    final Table<? extends Record> stats = this.statisticsSelection.selectEmployeesStats().asTable();
    return this.ctx.fetch(this.historicise(DSL.select(stats.fields()).from(stats).leftOuterJoin(employees)
        .on(stats.field(TRAININGS_EMPLOYEES.TREM_EMPL_FK).eq(employees.field(EMPLOYEES.EMPL_PK))).asTable()));
  }

  @GET
  @Path("sites/{site}")
  public Result<? extends Record> getSitesStatsHistory() {
    final Table<Record> sites = this.resourcesSelection.selectSites().asTable();
    final Table<? extends Record> stats = this.statisticsSelection.selectSitesStats().asTable();
    return this.ctx.fetch(this.historicise(DSL.select(stats.fields()).from(sites).leftOuterJoin(stats)
        .on(stats.field(SITES_EMPLOYEES.SIEM_SITE_FK).eq(sites.field(SITES.SITE_PK))).asTable()));
  }

  @GET
  @Path("sites-groups")
  public Result<? extends Record> getSitesGroupsStatsHistory() {
    if (!this.parameters.isDefault(QueryParameters.GROUP_BY_FIELD)) {
      throw new IllegalArgumentException("Statistics history generation may not be used with the 'group-by' parameter.");
    }

    final Table<? extends Record> sites = this.resourcesSelection.selectSites().asTable();
    try (final SelectQuery<? extends Record> stats = this.statisticsSelection.selectSitesGroupsStats()) {
      stats.addJoin(sites, JoinType.RIGHT_OUTER_JOIN, sites.field(SITES.SITE_PK).eq(Constants.unqualify(SITES_EMPLOYEES.SIEM_SITE_FK)));
      return this.ctx.fetch(this.historicise(stats.asTable()));
    }
  }

  private SelectQuery<? extends Record> historicise(final Table<? extends Record> stats) {
    final Table<Record1<@NonNull Date>> dates = DSL.select(this.date).asTable();
    return DSL
        .select(dates.field(this.date))
        .select(ResourcesHelper.jsonbObjectAggNullSafe(stats.field(CERTIFICATES.CERT_PK), stats.fields()).as("stats"))
        .from(dates)
        .leftOuterJoin(DSL.lateral(stats)).on(DSL.trueCondition()) // TODO: use DSL#noCondition when upgrading jOOQ
        .groupBy(this.date)
        .orderBy(this.date.asc()) // TODO: should this be sorted automatically, or left to the ResourcesCollator?
        .getQuery();
  }
}
