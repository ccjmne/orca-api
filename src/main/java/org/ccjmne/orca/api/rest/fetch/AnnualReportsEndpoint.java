package org.ccjmne.orca.api.rest.fetch;

import static org.ccjmne.orca.jooq.codegen.Tables.UPDATES;

import java.time.LocalDate;
import java.time.Month;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import org.ccjmne.orca.api.inject.business.Restrictions;
import org.jooq.DSLContext;
import org.jooq.DatePart;
import org.jooq.impl.DSL;

@Path("annual-reports")
public class AnnualReportsEndpoint {

  private final DSLContext       ctx;
  private final ResourcesEndpoint resources;

  @Inject
  public AnnualReportsEndpoint(final DSLContext ctx, final ResourcesEndpoint resources, final Restrictions restrictions) {
    if (!restrictions.canAccessAllSites()) {
      throw new ForbiddenException();
    }

    this.ctx = ctx;
    this.resources = resources;
  }

  @GET
  public Map<String, Object> getTodaysReport() {
    return this.getReport();
  }

  @GET
  @Path("{date}")
  public Map<String, Object> getReport() {
    final Map<String, Object> report = this.resources.lookupGlobalSitesGroup().intoMap();
    report.put("stats", report.remove("sgrp_stats"));
    return report;
  }

  @GET
  @Path("suggested-dates")
  public List<LocalDate> getSuggestedDates() {
    final int min = this.ctx.select(DSL.extract(DSL.min(UPDATES.UPDT_DATE), DatePart.YEAR)).from(UPDATES).fetchSingle().value1();
    final LocalDate today = LocalDate.now();
    return Stream.concat(IntStream.range(min, today.getYear()).mapToObj(y -> LocalDate.of(y, Month.DECEMBER, 31)), Stream.of(today))
        .collect(Collectors.toList());
  }
}
