package org.ccjmne.orca.api.rest.utils;

import static org.ccjmne.orca.jooq.codegen.Tables.CERTIFICATES;
import static org.ccjmne.orca.jooq.codegen.Tables.EMPLOYEES;
import static org.ccjmne.orca.jooq.codegen.Tables.SITES;
import static org.ccjmne.orca.jooq.codegen.Tables.SITES_TAGS;
import static org.ccjmne.orca.jooq.codegen.Tables.TAGS;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGS;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGS_TRAINERS;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGTYPES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGTYPES_CERTIFICATES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGTYPES_DEFS;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import org.ccjmne.orca.api.inject.business.QueryParams;
import org.ccjmne.orca.api.inject.core.ResourcesSelection;
import org.ccjmne.orca.api.utils.Fields;
import org.ccjmne.orca.api.utils.JSONFields;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Param;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SelectQuery;
import org.jooq.Table;
import org.jooq.impl.DSL;

import com.google.common.collect.ImmutableMap;

@Path("quick-search")
public class QuickSearchEndpoint {

  private static final int           LIMIT                = 8;
  private static final Field<Double> FIELD_DISTANCE       = DSL.field("distance", Double.class);
  private static final Field<Integer> FIELD_DATE_DISTANCE = DSL.field("date_distance", Integer.class);
  private static final Field<String> FIELD_SITA_VALUE_RAW = SITES_TAGS.SITA_VALUE.as("value_raw");

  /**
   * These very specific fields are covered by <b>GiST indexes</b> in the
   * database.
   */
  private static final Map<String, Field<?>[]> FIELDS = ImmutableMap
      .of("employee", new Field<?>[] { EMPLOYEES.EMPL_EXTERNAL_ID, EMPLOYEES.EMPL_SURNAME, EMPLOYEES.EMPL_FIRSTNAME, EMPLOYEES.EMPL_NOTES },
          "site", new Field<?>[] { SITES.SITE_EXTERNAL_ID, SITES.SITE_NAME, SITES.SITE_NOTES },
          "site-tag", new Field<?>[] { TAGS.TAGS_SHORT, TAGS.TAGS_NAME, FIELD_SITA_VALUE_RAW },
          "session", new Field<?>[] {});

  public static final List<String> RESOURCES_TYPES = new ArrayList<>(FIELDS.keySet());

  private final DSLContext         ctx;
  private final ResourcesSelection resourcesSelection;
  private final QueryParams        parameters;
  private final List<String>       resourcesTypes;
  private final Param<String>      searchTerms;
  private final Field<LocalDate>   sessionDate;

  @Inject
  private QuickSearchEndpoint(final DSLContext ctx, final ResourcesSelection resourcesSelection, final QueryParams parameters) {
    this.ctx = ctx;
    this.resourcesSelection = resourcesSelection;
    this.parameters = parameters;
    this.resourcesTypes = parameters.get(QueryParams.RESOURCE_TYPE);
    this.searchTerms = parameters.getOrDefault(QueryParams.SEARCH_TERMS, DSL.val(""));
    this.sessionDate = parameters.get(QueryParams.SESSION_DATE);
  }

  @GET
  public List<Record> search() {
    return this.resourcesTypes.parallelStream()
        .flatMap(type -> this.searchImpl(type).stream())
        .sorted(Comparator.comparing(r -> r.get(FIELD_DISTANCE)))
        .limit(LIMIT).collect(Collectors.toList());
  }

  @GET
  @Path("{type}")
  public List<Record> searchForType() {
    return this.searchImpl(this.resourcesTypes.get(0));
  }

  private Result<Record> searchImpl(final String type) {
    final Table<? extends Record> table;
    switch (type) {
      case "employee":
        table = this.resourcesSelection.scopeEmployees().asTable();
        break;
      case "site":
        table = this.resourcesSelection.scopeSites().asTable();
        break;
      case "site-tag":
        table = DSL
            .selectDistinct(TAGS.fields())
            .select(FIELD_SITA_VALUE_RAW, Fields.TAG_VALUE_COERCED.as("sita_value"))
            .from(TAGS)
            .join(SITES_TAGS).on(SITES_TAGS.SITA_TAGS_FK.eq(TAGS.TAGS_PK)
                .and(SITES_TAGS.SITA_SITE_FK.in(Fields.select(SITES.SITE_PK, this.resourcesSelection.scopeSites()))))
            .asTable();
        break;
      case "session":
        // Searching for sessions additionally matches according to chronological
        // proximity to a given Date.
        return this.searchSessions();
      default:
        throw new IllegalArgumentException(String.format("Only the following resource types are supported: %s", RESOURCES_TYPES));
    }

    // This very specific expression is used in GiST indexes for each resource type
    final Field<Double> distance = QuickSearchEndpoint.wordDistance(this.searchTerms, QuickSearchEndpoint
        .unaccent(QuickSearchEndpoint.concatWS(table.fields(FIELDS.get(type)))));
    return this.ctx
        .select(distance.as(FIELD_DISTANCE))
        .select(table.fields())
        .from(table)
        .where(distance.lessOrEqual(Double.valueOf(.5)))
        .and(this.parameters.isEnabled(QueryParams.TRAINERS_ONLY) ? table.field(EMPLOYEES.EMPL_PK)
            .in(DSL.select(TRAININGS_TRAINERS.TRTR_EMPL_FK).from(TRAININGS_TRAINERS))
            : DSL.trueCondition())
        .orderBy(distance)
        .limit(LIMIT).fetch();
  }

  /**
   * Searching for sessions is a special case, which uses both:
   * <ul>
   * <li>Fuzzy string matching on the session type and associated
   * certificates</li>
   * <li>Chronological proximity matching to a given Date</li>
   * </ul>
   */
  private Result<Record> searchSessions() {
    final Table<Record> sessions = this.resourcesSelection.scopeSessions().asTable();

    final Table<Record> searchableSessions = DSL
        .select(sessions.fields())
        .select(TRAININGTYPES.fields())
        .select(QuickSearchEndpoint.stringAgg(CERTIFICATES.CERT_SHORT).as("shorts"))
        .select(QuickSearchEndpoint.stringAgg(CERTIFICATES.CERT_NAME).as("certs"))
        .from(sessions)
        .join(TRAININGTYPES).on(TRAININGTYPES.TRTY_PK.eq(sessions.field(TRAININGS.TRNG_TRTY_FK)))
        .join(TRAININGTYPES_DEFS).on(TRAININGTYPES_DEFS.TTDF_PK.eq(Fields
            .selectTypeDefinition(sessions.field(TRAININGS.TRNG_TRTY_FK), sessions.field(TRAININGS.TRNG_DATE))))
        .leftJoin(TRAININGTYPES_CERTIFICATES).on(TRAININGTYPES_CERTIFICATES.TTCE_TTDF_FK.eq(TRAININGTYPES_DEFS.TTDF_PK))
        .leftJoin(CERTIFICATES).on(CERTIFICATES.CERT_PK.eq(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK))
        .groupBy(Fields.concat(sessions.fields(), TRAININGTYPES.fields()))
        .asTable();

    try (final SelectQuery<Record> query = this.ctx.selectQuery()) {
      final Field<Double> distance;
      if (this.searchTerms.getValue().isEmpty() && !this.parameters.isDefault(QueryParams.SESSION_DATE)) {
        // if no SEARCH_TERMS but SESSION_DATE is specified, accept any session type
        distance = DSL.val(Double.valueOf(1));
      } else {
        distance = QuickSearchEndpoint
            .wordDistance(this.searchTerms, QuickSearchEndpoint.unaccent(QuickSearchEndpoint.concatWS(searchableSessions.fields("trty_name", "shorts", "certs"))));
        query.addConditions(distance.le(Double.valueOf(.5)));
      }

      final Field<Integer> dateDistance = DSL.abs(DSL.localDateDiff(searchableSessions.field(TRAININGS.TRNG_DATE), this.sessionDate)).as(FIELD_DATE_DISTANCE);
      query.addSelect(dateDistance);
      query.addSelect(searchableSessions.fields(TRAININGS.fields()));
      query.addSelect(distance.as(FIELD_DISTANCE));
      query.addSelect(JSONFields.toJson(searchableSessions.fields(TRAININGTYPES.fields())).as("type"));
      query.addFrom(searchableSessions);
      if (!this.parameters.isDefault(QueryParams.FROM) || !this.parameters.isDefault(QueryParams.TO)) {
        query.addConditions(searchableSessions.field(TRAININGS.TRNG_DATE).between(
                                                                                 this.parameters.get(QueryParams.FROM),
                                                                                 this.parameters.get(QueryParams.TO)));
      }
      query.addOrderBy(distance, dateDistance);
      query.addLimit(LIMIT);
      return query.fetch();
    }
  }

  private static Field<Double> wordDistance(final Field<String> search, final Field<String> indexedExpression) {
    return DSL.field("{0} <<-> {1}", Double.class, QuickSearchEndpoint.unaccent(search), indexedExpression);
  }

  /**
   * Aggregates the given {@code Field} into a {@code " "}-joined {@code String}
   * for the selected records.
   */
  private static Field<String> stringAgg(final Field<String> field) {
    return DSL.field("string_agg({0}, ' ')", String.class, field);
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
