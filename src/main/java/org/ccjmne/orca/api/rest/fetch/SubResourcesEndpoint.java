package org.ccjmne.orca.api.rest.fetch;

import static org.ccjmne.orca.jooq.codegen.Tables.CERTIFICATES;
import static org.ccjmne.orca.jooq.codegen.Tables.SITES;
import static org.ccjmne.orca.jooq.codegen.Tables.SITES_TAGS;
import static org.ccjmne.orca.jooq.codegen.Tables.TAGS;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGS;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGTYPES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGTYPES_CERTIFICATES;

import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import org.ccjmne.orca.api.inject.business.QueryParams;
import org.ccjmne.orca.api.inject.business.Restrictions;
import org.ccjmne.orca.api.inject.core.ResourcesSelection;
import org.ccjmne.orca.api.utils.Fields;
import org.ccjmne.orca.api.utils.JSONFields;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record3;
import org.jooq.Table;
import org.jooq.impl.DSL;

/**
 * Serves the resources whose access isn't restricted.<br />
 * Presents resources into {@link Map}s keyed by their unique identifier.
 *
 * @author ccjmne
 */
@Path("sub-resources")
public class SubResourcesEndpoint {

  private final DSLContext         ctx;
  private final ResourcesSelection resourcesSelection;
  private final Restrictions       restrictions;

  private final boolean adminHints;

  @Inject
  public SubResourcesEndpoint(
                              final DSLContext ctx,
                              final ResourcesSelection resourcesSelection,
                              final QueryParams parameters,
                              final Restrictions restrictions) {
    this.ctx = ctx;
    this.resourcesSelection = resourcesSelection;
    this.restrictions = restrictions;
    this.adminHints = parameters.isEnabled(QueryParams.INCLUDE_ADMIN_HINTS);
  }

  /**
   * Enabling {@link QueryParams#INCLUDE_ADMIN_HINTS} adds associated
   * session-types count.
   */
  @GET
  @Path("certificates")
  public Map<Integer, ? extends Record> getCertificates() {
    if (this.adminHints && !this.restrictions.canManageCertificates()) {
      throw new ForbiddenException("This account is not allowed to enable admin hints for this request.");
    }

    return this.adminHints
                           ? this.ctx
                               .select(CERTIFICATES.fields())
                               .select(JSONFields.arrayAggOrderBy(TRAININGTYPES.TRTY_ORDER.asc(), TRAININGTYPES.fields()).as("session_types"))
                               .from(CERTIFICATES)
                               .leftJoin(TRAININGTYPES_CERTIFICATES).on(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK.eq(CERTIFICATES.CERT_PK))
                               .leftJoin(TRAININGTYPES).on(TRAININGTYPES.TRTY_PK.eq(TRAININGTYPES_CERTIFICATES.TTCE_TRTY_FK))
                               .groupBy(CERTIFICATES.fields())
                               .fetchMap(CERTIFICATES.CERT_PK)
                           : this.ctx.selectFrom(CERTIFICATES).fetchMap(CERTIFICATES.CERT_PK);
  }

  /**
   * Enabling {@link QueryParams#INCLUDE_ADMIN_HINTS} adds associated sessions
   * count.
   */
  @GET
  @Path("session-types")
  public Map<Integer, ? extends Record> getSessionTypes() {
    if (this.adminHints && !this.restrictions.canManageCertificates()) {
      throw new ForbiddenException("This account is not allowed to enable admin hints for this request.");
    }

    final Table<Record> types = DSL
        .select(TRAININGTYPES.fields())
        .select(JSONFields.arrayAggOrderBy(CERTIFICATES.CERT_ORDER.asc(), Fields.concat(CERTIFICATES.fields(), TRAININGTYPES_CERTIFICATES.TTCE_DURATION))
            .as("certificates"))
        .from(TRAININGTYPES)
        .leftJoin(TRAININGTYPES_CERTIFICATES).on(TRAININGTYPES_CERTIFICATES.TTCE_TRTY_FK.eq(TRAININGTYPES.TRTY_PK))
        .leftJoin(CERTIFICATES).on(CERTIFICATES.CERT_PK.eq(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK))
        .groupBy(TRAININGTYPES.fields())
        .asTable();

    return this.adminHints
                           ? this.ctx
                               .select(types.fields())
                               .select(DSL.count(TRAININGS.TRNG_PK).as("sessions_count"))
                               .from(types)
                               .leftJoin(TRAININGS).on(TRAININGS.TRNG_TRTY_FK.eq(types.field(TRAININGTYPES.TRTY_PK)))
                               .groupBy(types.fields())
                               .fetchMap(types.field(TRAININGTYPES.TRTY_PK))
                           : this.ctx.select(types.fields()).from(types).fetchMap(types.field(TRAININGTYPES.TRTY_PK));
  }

  @GET
  @Path("tags")
  public Map<Integer, ? extends Record> getTags() {
    final Table<Record3<Integer, String, Integer>> stats = DSL
        .select(SITES_TAGS.SITA_TAGS_FK, SITES_TAGS.SITA_VALUE, DSL.count(SITES_TAGS.SITA_VALUE))
        .from(SITES_TAGS)
        .where(SITES_TAGS.SITA_SITE_FK.in(Fields.select(SITES.SITE_PK, this.resourcesSelection.scopeSites())))
        .groupBy(SITES_TAGS.SITA_TAGS_FK, SITES_TAGS.SITA_VALUE)
        .asTable();

    return this.ctx
        .select(TAGS.fields())
        .select(DSL.sum(stats.field(2, Integer.class)).as("tags_sites_count"))
        .select(JSONFields.objectAgg(stats.field(1), stats.field(2)).as("tags_values_counts"))
        .select(JSONFields.arrayAgg(Fields.TAG_VALUE_COERCED).as("tags_values"))
        .from(TAGS)
        .leftOuterJoin(stats).on(stats.field(SITES_TAGS.SITA_TAGS_FK).eq(TAGS.TAGS_PK))
        .groupBy(TAGS.fields())
        .fetchMap(TAGS.TAGS_PK);
  }
}
