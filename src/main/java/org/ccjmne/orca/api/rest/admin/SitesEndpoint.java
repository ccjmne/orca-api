package org.ccjmne.orca.api.rest.admin;

import static org.ccjmne.orca.jooq.codegen.Tables.SITES;
import static org.ccjmne.orca.jooq.codegen.Tables.SITES_EMPLOYEES;
import static org.ccjmne.orca.jooq.codegen.Tables.SITES_TAGS;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

import org.ccjmne.orca.api.inject.business.QueryParams;
import org.ccjmne.orca.api.inject.business.Restrictions;
import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.api.utils.Transactions;
import org.ccjmne.orca.jooq.codegen.Sequences;
import org.ccjmne.orca.jooq.codegen.tables.records.SitesTagsRecord;
import org.jooq.DSLContext;

import com.google.common.base.MoreObjects;

@Path("sites")
public class SitesEndpoint {

  private final DSLContext  ctx;
  private final QueryParams parameters;

  @Inject
  public SitesEndpoint(final DSLContext ctx, final Restrictions restrictions, final QueryParams parameters) {
    if (!restrictions.canManageSitesAndTags()) {
      throw new ForbiddenException();
    }

    this.ctx = ctx;
    this.parameters = parameters;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Integer createSite(final Map<String, Object> site) {
    final Integer site_pk = new Integer(this.ctx.nextval(Sequences.SITES_SITE_PK_SEQ).intValue());
    this.upsertSite(site_pk, site);
    return site_pk;
  }

  /**
   * @return <code>true</code> iff that site was not existing and thus,
   *         actually created
   */
  @PUT
  @Path("{site}")
  @Consumes(MediaType.APPLICATION_JSON)
  public Boolean updateSite(final Map<String, Object> site) {
    return this.upsertSite(this.parameters.getRaw(QueryParams.SITE), site);
  }

  /**
   * @return <code>true</code> iff that site was existing and thus, actually
   *         deleted
   */
  @DELETE
  @Path("{site}")
  public Boolean deleteSite() {
    final Integer site = this.parameters.getRaw(QueryParams.SITE);
    // Database CASCADEs the deletion of linked users, if any
    // Database CASCADEs the deletion of its tags
    return Transactions.with(this.ctx, transaction -> {
      // Set deleted entity's employees' site to DECOMMISSIONED_SITE
      final boolean exists = transaction.fetchExists(SITES, SITES.SITE_PK
          .ne(Constants.DECOMMISSIONED_SITE)
          .and(SITES.SITE_PK.eq(site)));
      if (exists) {
        transaction.update(SITES_EMPLOYEES)
            .set(SITES_EMPLOYEES.SIEM_SITE_FK, Constants.DECOMMISSIONED_SITE)
            .where(SITES_EMPLOYEES.SIEM_SITE_FK.eq(site)).execute();
        transaction.delete(SITES).where(SITES.SITE_PK.eq(site)).execute();
      }

      return Boolean.valueOf(exists);
    });
  }

  private Boolean upsertSite(final Integer site_pk, final Map<String, Object> site) {
    return Transactions.with(this.ctx, transaction -> {
      @SuppressWarnings("unchecked")
      final Map<String, Object> tags = MoreObjects.firstNonNull((Map<String, Object>) site.remove("site_tags"), Collections.emptyMap());
      final boolean exists = transaction.fetchExists(SITES, SITES.SITE_PK.eq(site_pk));
      if (exists) {
        transaction.update(SITES).set(site).where(SITES.SITE_PK.eq(site_pk)).execute();
      } else {
        transaction.insertInto(SITES).set(site).set(SITES.SITE_PK, site_pk).execute();
      }

      transaction.deleteFrom(SITES_TAGS).where(SITES_TAGS.SITA_SITE_FK.eq(site_pk)).execute();
      transaction.batchInsert(tags.entrySet().stream()
          .map(tag -> {
            // TODO: Prevent insertion of non-boolean values for b-type tags
            // TODO: Prevent insertion of 'true' and 'false' for s-type tags
            if (Constants.TAGS_VALUE_NONE.equals(tag.getValue()) || Constants.TAGS_VALUE_UNIVERSAL.equals(tag.getValue())) {
              throw new IllegalArgumentException(String.format("Invalid tag value: '%s'", tag.getValue()));
            }

            return new SitesTagsRecord(site_pk, Integer.valueOf(tag.getKey()), String.valueOf(tag.getValue()));
          }).collect(Collectors.toList())).execute();
      return Boolean.valueOf(!exists);
    });
  }
}
