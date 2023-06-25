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
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import org.ccjmne.orca.api.modules.Restrictions;
import org.ccjmne.orca.api.utils.Constants;
import org.jooq.DSLContext;
import org.jooq.DatePart;
import org.jooq.impl.DSL;

import com.google.common.collect.ImmutableMap;

@Path("annual-reports")
public class AnnualReportsEndpoint {

    private final DSLContext ctx;
    private final StatisticsEndpoint stats;
    private ResourcesEndpoint resources;

    @Inject
    public AnnualReportsEndpoint(
                                 final DSLContext ctx,
                                 final StatisticsEndpoint stats,
                                 final ResourcesEndpoint resources,
                                 final ResourcesByKeysCommonEndpoint commonResources,
                                 final Restrictions restrictions) {
        if (!restrictions.canAccessAllSites()) {
            throw new ForbiddenException();
        }

        this.ctx = ctx;
        this.stats = stats;
        this.resources = resources;
    }

    @GET
    public Map<? extends Object, Object> getTodaysReport(@Context final UriInfo uriInfo) {
        return this.getReport(null, uriInfo);
    }

    @GET
    @Path("{date}")
    public Map<? extends Object, Object> getReport(@PathParam("date") final String dateStr, @Context final UriInfo uriInfo) {
        return ImmutableMap.builder()
                .putAll(this.resources.listSitesGroups(null, dateStr, false, uriInfo).get(0))
                .put("stats", this.stats.getSitesGroupsStats(null, dateStr, uriInfo).get(Constants.TAGS_VALUE_UNIVERSAL))
                .build();
    }

    @GET
    @Path("suggested-dates")
    public List<LocalDate> getSuggestedDates() {
        final Integer min = this.ctx.select(DSL.extract(DSL.min(UPDATES.UPDT_DATE), DatePart.YEAR)).from(UPDATES).fetchOne().value1();
        final LocalDate today = LocalDate.now();
        return Stream
                .concat(IntStream.range(min.intValue(), today.getYear()).mapToObj(year -> LocalDate.of(year, Month.DECEMBER, 31)), Stream.of(today))
                .collect(Collectors.toList());
    }

}
