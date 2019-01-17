package org.ccjmne.orca.api.rest.fetch;

import static org.ccjmne.orca.jooq.classes.Tables.CERTIFICATES;
import static org.ccjmne.orca.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGTYPES_CERTIFICATES;

import java.sql.Date;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import org.ccjmne.orca.api.inject.business.QueryParams;
import org.ccjmne.orca.api.inject.core.ResourcesSelection;
import org.ccjmne.orca.api.inject.core.StatisticsSelection;
import org.ccjmne.orca.api.utils.Fields;
import org.ccjmne.orca.api.utils.JSONFields;
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

import jersey.repackaged.com.google.common.base.Objects;

/**
 * Compute statistics for core resources at multiple points in time.
 *
 * @author ccjmne
 */
@Path("statistics-over-time")
public class StatisticsOverTimeEndpoint {

  private static final String DATE_SERIES_FIELD_NAME = "date";

  private final DSLContext          ctx;
  private final QueryParams         parameters;
  private final ResourcesSelection  resourcesSelection;
  private final StatisticsSelection statisticsSelection;

  private final Field<@NonNull Date> date;

  @Inject
  private StatisticsOverTimeEndpoint(final DSLContext ctx, final QueryParams parameters, final ResourcesSelection resourcesSelection) {
    this.ctx = ctx;
    this.parameters = parameters;
    this.resourcesSelection = resourcesSelection;
    this.statisticsSelection = new StatisticsSelection(DSL.field(DATE_SERIES_FIELD_NAME, Date.class));
    this.date = DSL
        .field("generate_series(date_trunc({2}, {0}), {1}, CONCAT('1 ', {2})::interval)::date", Date.class,
               parameters.get(QueryParams.FROM),
               parameters.get(QueryParams.TO),
               parameters.get(QueryParams.INTERVAL))
        .as(DATE_SERIES_FIELD_NAME);
  }

  @GET
  @Path("employees/{employee}")
  public Result<? extends Record> getEmployeesStatsOverTime() {
    final Table<Record> employees = this.resourcesSelection.scopeEmployees().asTable();
    final Table<? extends Record> stats = this.statisticsSelection.selectEmployeesStats().asTable();
    return this.ctx.fetch(this.seriesify(DSL.select().from(employees).leftOuterJoin(stats)
        .on(stats.field(TRAININGS_EMPLOYEES.TREM_EMPL_FK).eq(employees.field(EMPLOYEES.EMPL_PK))).asTable(), Fields.EMPLOYEES_STATS_FIELDS));
  }

  @GET
  @Path("sites/{site}")
  public Result<? extends Record> getSitesStatsOverTime() {
    final Table<Record> sites = this.resourcesSelection.scopeSites().asTable();
    final Table<? extends Record> stats = this.statisticsSelection.selectSitesStats().asTable();
    return this.ctx.fetch(this.seriesify(DSL.select().from(sites).leftOuterJoin(stats)
        .on(stats.field(SITES_EMPLOYEES.SIEM_SITE_FK).eq(sites.field(SITES.SITE_PK))).asTable(), Fields.SITES_STATS_FIELDS));
  }

  @GET
  @Path("sites-groups")
  public Result<? extends Record> getSitesGroupsStatsOverTime() {
    if (!this.parameters.isDefault(QueryParams.GROUP_BY_FIELD)) {
      throw new IllegalArgumentException("Statistics history generation may not be used with the 'group-by' parameter.");
    }

    final Table<? extends Record> sites = this.resourcesSelection.selectSites().asTable();
    try (final SelectQuery<? extends Record> stats = this.statisticsSelection.selectSitesGroupsStats()) {
      stats.addJoin(sites, JoinType.RIGHT_OUTER_JOIN, sites.field(SITES.SITE_PK).eq(Fields.unqualify(SITES_EMPLOYEES.SIEM_SITE_FK)));
      return this.ctx.fetch(this.seriesify(stats.asTable(), Fields.SITES_GROUPS_STATS_FIELDS));
    }
  }

  private SelectQuery<? extends Record> seriesify(final Table<? extends Record> stats, final String[] fields) {
    final Table<Record1<@NonNull Date>> dates = DSL.select(this.date).asTable();
    return DSL
        .select(dates.field(this.date))
        .select(JSONFields
            .objectAgg(Objects.firstNonNull(stats.field(CERTIFICATES.CERT_PK), stats.field(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK)), stats.fields(fields))
            .as("stats"))
        .from(dates)
        .leftOuterJoin(DSL.lateral(stats)).on(DSL.noCondition())
        .groupBy(this.date)
        .orderBy(this.date)
        .getQuery();
  }
}
