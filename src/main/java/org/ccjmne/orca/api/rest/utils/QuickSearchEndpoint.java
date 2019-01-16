package org.ccjmne.orca.api.rest.utils;

import static org.ccjmne.orca.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import org.ccjmne.orca.api.inject.business.QueryParams;
import org.ccjmne.orca.api.inject.core.ResourcesSelection;
import org.ccjmne.orca.api.utils.Test;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Param;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Table;
import org.jooq.impl.DSL;

import com.google.common.collect.ImmutableMap;

@Path("quick-search")
public class QuickSearchEndpoint {

  // TODO: Support "sessions" and "sites-tags" at least
  public static final List<String> RESOURCES_TYPES = Arrays.asList("employees", "sites");// , "sessions", "sites-tags");
  private static final int         LIMIT           = 8;
  private static final String      FIELD_DISTANCE  = "distance";

  /**
   * These very specific fields are covered by <b>GiST indexes</b> in the
   * database.
   */
  private static final Map<String, Field<?>[]> FIELDS = ImmutableMap
      .of("employees", new Field<?>[] { EMPLOYEES.EMPL_EXTERNAL_ID, EMPLOYEES.EMPL_SURNAME, EMPLOYEES.EMPL_FIRSTNAME, EMPLOYEES.EMPL_NOTES },
          "sites", new Field<?>[] { SITES.SITE_EXTERNAL_ID, SITES.SITE_NAME, SITES.SITE_NOTES });

  private final DSLContext         ctx;
  private final ResourcesSelection resourcesSelection;
  private final List<String>       resourcesTypes;
  private final Param<String>      searchTerms;

  @Inject
  private QuickSearchEndpoint(final DSLContext ctx, final ResourcesSelection resourcesSelection, final QueryParams parameters) {
    this.ctx = ctx;
    this.resourcesSelection = resourcesSelection;
    this.resourcesTypes = parameters.get(QueryParams.RESOURCE_TYPE);
    this.searchTerms = parameters.get(QueryParams.SEARCH_TERMS);
  }

  @GET
  public List<Record> search() {
    return this.resourcesTypes.parallelStream()
        .flatMap(type -> this.searchImpl(type).stream())
        .sorted(Comparator.comparing(r -> r.get(FIELD_DISTANCE, Double.class)))
        .limit(LIMIT).collect(Collectors.toList());
  }

  @GET
  @Path("{type}")
  public List<Record> searchForType() {
    return this.searchImpl(this.resourcesTypes.get(0));
  }

  private Result<Record> searchImpl(final String type) {
    final Table<Record> table;
    switch (type) {
      case "employees":
        table = this.resourcesSelection.scopeEmployees().asTable();
        break;
      case "sites":
        table = this.resourcesSelection.scopeSites().asTable();
        break;
      default:
        throw new IllegalArgumentException(String.format("Only the following resource types are supported: %s", RESOURCES_TYPES));
    }

    // This very specific expression is used in GiST indexes for each resource type
    final Field<Double> distance = QuickSearchEndpoint.wordDistance(this.searchTerms, QuickSearchEndpoint
        .unaccent(QuickSearchEndpoint.concatWS(table.fields(FIELDS.get(type)))));
    return this.ctx
        .select(distance.as(FIELD_DISTANCE)).select(table.fields()).from(table)
        .where(distance.lessOrEqual(new Double(.5)))
        .orderBy(distance).limit(8)
        .fetch();
  }

  private static Field<Double> wordDistance(final Field<String> search, final Field<String> indexedExpression) {
    return DSL.field("{0} <<-> {1}", Double.class, Test.unaccent(search), indexedExpression);
  }

  /**
   * Special function {@code public.f_unaccent} that is {@code IMMUTABLE} and
   * simply delegates to {@code public.unaccent}.
   */
  private static Field<String> unaccent(final Field<String> field) {
    return DSL.function("f_unaccent", String.class, field);
  }

  /**
   * Special function {@code pg_catalog.f_concat_ws} that is {@code IMMUTABLE} and
   * simply delegates to {@code pg_catalog.concat_ws}.
   */
  private static Field<String> concatWS(final Field<?>... fields) {
    return DSL.function("f_concat_ws", String.class, fields);
  }
}
