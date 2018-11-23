package org.ccjmne.orca.api.rest.admin;

import static org.ccjmne.orca.jooq.classes.Tables.SITES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES_TAGS;

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
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.ObjectUtils;
import org.ccjmne.orca.api.inject.Restrictions;
import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.api.utils.Transactions;
import org.ccjmne.orca.jooq.classes.Sequences;
import org.ccjmne.orca.jooq.classes.tables.records.SitesTagsRecord;
import org.jooq.DSLContext;

@Path("sites")
public class SitesEndpoint {

  private final DSLContext ctx;

  @Inject
  public SitesEndpoint(final DSLContext ctx, final Restrictions restrictions) {
    if (!restrictions.canManageSitesAndTags()) {
      throw new ForbiddenException();
    }

    this.ctx = ctx;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Integer createSite(final Map<String, Object> siteDefinition) {
    final Integer site_pk = new Integer(this.ctx.nextval(Sequences.SITES_SITE_PK_SEQ).intValue());
    this.updateSite(site_pk, siteDefinition);
    return site_pk;
  }

  /**
   * @return <code>true</code> iff that site was not existing and thus,
   *         actually created
   */
  @PUT
  @Path("{site_pk}")
  @Consumes(MediaType.APPLICATION_JSON)
  public Boolean updateSite(@PathParam("site_pk") final Integer site_pk, final Map<String, Object> siteDefinition) {
    return Transactions.with(this.ctx, transactionCtx -> {
      @SuppressWarnings("unchecked")
      final Map<String, Object> tags = ObjectUtils.defaultIfNull((Map<String, Object>) siteDefinition.remove("tags"), Collections.emptyMap());
      final boolean exists = transactionCtx.fetchExists(SITES, SITES.SITE_PK.eq(site_pk));
      if (exists) {
        transactionCtx.update(SITES).set(siteDefinition).where(SITES.SITE_PK.eq(site_pk)).execute();
      } else {
        transactionCtx.insertInto(SITES).set(siteDefinition).set(SITES.SITE_PK, site_pk).execute();
      }

      transactionCtx.deleteFrom(SITES_TAGS).where(SITES_TAGS.SITA_SITE_FK.eq(site_pk)).execute();
      transactionCtx.batchInsert(tags.entrySet().stream()
          .map(tag -> {
            // TODO: Prevent insertion of non-boolean values for
            // 'b'-type tags
            if (Constants.TAGS_VALUE_NONE.equals(tag.getValue()) || Constants.TAGS_VALUE_UNIVERSAL.equals(tag.getValue())) {
              throw new IllegalArgumentException(String.format("Invalid tag value: '%s'", tag.getValue()));
            }

            return new SitesTagsRecord(site_pk, Integer.valueOf(tag.getKey()), String.valueOf(tag.getValue()));
          }).collect(Collectors.toList())).execute();
      return Boolean.valueOf(!exists);
    });
  }

  /**
   * @return <code>true</code> iff that site was existing and thus, actually
   *         deleted
   */
  @DELETE
  @Path("{site_pk}")
  public Boolean deleteSite(@PathParam("site_pk") final Integer site_pk) {
    // Database CASCADEs the deletion of linked users, if any
    // Database CASCADEs the deletion of its tags
    return Transactions.with(this.ctx, transactionCtx -> {
      // Set deleted entity's employees' site to DECOMMISSIONED_SITE
      final boolean exists = transactionCtx.fetchExists(SITES, SITES.SITE_PK
          .ne(Constants.DECOMMISSIONED_SITE)
          .and(SITES.SITE_PK.eq(site_pk)));
      if (exists) {
        transactionCtx.update(SITES_EMPLOYEES)
            .set(SITES_EMPLOYEES.SIEM_SITE_FK, Constants.DECOMMISSIONED_SITE)
            .where(SITES_EMPLOYEES.SIEM_SITE_FK.eq(site_pk)).execute();
        transactionCtx.delete(SITES).where(SITES.SITE_PK.eq(site_pk)).execute();
      }

      return Boolean.valueOf(exists);
    });
  }

}
