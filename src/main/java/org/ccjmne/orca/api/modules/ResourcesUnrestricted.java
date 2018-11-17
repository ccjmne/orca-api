package org.ccjmne.orca.api.modules;

import static org.ccjmne.orca.jooq.classes.Tables.CERTIFICATES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES_TAGS;
import static org.ccjmne.orca.jooq.classes.Tables.TAGS;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGTYPES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGTYPES_CERTIFICATES;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.api.utils.ResourcesHelper;
import org.ccjmne.orca.api.utils.ResourcesSelection;
import org.ccjmne.orca.jooq.classes.tables.records.CertificatesRecord;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JoinType;
import org.jooq.Result;
import org.jooq.Select;
import org.jooq.impl.DSL;

/**
 * Concentrate all accesses to the database (all usages of a {@link DSLContext})
 * that are not meant to be filtered according to the {@link Restrictions}
 * module.
 */
public class ResourcesUnrestricted {

  // TODO: make private when rewriting training statistics computing
  public static final Field<Integer[]> TRAININGTYPE_CERTIFICATES = ResourcesHelper
      .arrayAggDistinctOmitNull(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK).as("certificates");

  private final DSLContext         ctx;
  private final ResourcesSelection resources;

  @Inject
  public ResourcesUnrestricted(final DSLContext ctx, final ResourcesSelection resources) {
    this.ctx = ctx;
    this.resources = resources;
  }

  public List<Map<String, Object>> listTrainingTypes() {
    return this.ctx.select(TRAININGTYPES.fields())
        .select(ResourcesHelper.arrayAgg(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK),
                ResourcesHelper.arrayAgg(TRAININGTYPES_CERTIFICATES.TTCE_DURATION))
        .from(TRAININGTYPES)
        .join(TRAININGTYPES_CERTIFICATES, JoinType.LEFT_OUTER_JOIN).on(TRAININGTYPES_CERTIFICATES.TTCE_TRTY_FK.eq(TRAININGTYPES.TRTY_PK))
        .groupBy(TRAININGTYPES.fields())
        .orderBy(TRAININGTYPES.TRTY_ORDER)
        .fetch(ResourcesHelper
            .getMapperWithZip(ResourcesHelper.getZipMapper(false,
                                                           TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK,
                                                           TRAININGTYPES_CERTIFICATES.TTCE_DURATION),
                              "certificates"));
  }

  public Result<CertificatesRecord> listCertificates() {
    return this.ctx.selectFrom(CERTIFICATES).orderBy(CERTIFICATES.CERT_ORDER).fetch();
  }

  // TODO: Rewrite using JSON_TYPE
  public List<Map<String, Object>> listTags() {
    try (
        final Select<?> valuesStats = DSL
            .select(DSL.count(SITES_TAGS.SITA_VALUE).as("count"), SITES_TAGS.SITA_VALUE, SITES_TAGS.SITA_TAGS_FK)
            .from(SITES_TAGS)
            .where(SITES_TAGS.SITA_SITE_FK.in(Constants
                .select(SITES.SITE_PK, this.resources.selectSites())))
            .groupBy(SITES_TAGS.SITA_TAGS_FK, SITES_TAGS.SITA_VALUE);
        final Select<?> tagsStats = DSL
            .select(DSL.arrayAgg(valuesStats.field(SITES_TAGS.SITA_VALUE)).as("values"),
                    DSL.arrayAgg(valuesStats.field("count")).as("counts"),
                    DSL.sum(valuesStats.field("count", Integer.class)).as("count"), valuesStats.field(SITES_TAGS.SITA_TAGS_FK))
            .from(valuesStats)
            .groupBy(valuesStats.field(SITES_TAGS.SITA_TAGS_FK))) {
      return this.ctx.select(TAGS.fields())
          .select(tagsStats.field("values"),
                  tagsStats.field("values").as("counts.key"),
                  tagsStats.field("counts").as("counts.value"),
                  tagsStats.field("count"))
          .from(TAGS).join(tagsStats, JoinType.LEFT_OUTER_JOIN).on(tagsStats.field(SITES_TAGS.SITA_TAGS_FK).eq(TAGS.TAGS_PK))
          .orderBy(TAGS.TAGS_ORDER)
          .fetch(ResourcesHelper
              .coercing(ResourcesHelper.getMapperWithZip(ResourcesHelper.getZipSelectMapper("counts.key", "counts.value"), "counts"),
                        ResourcesHelper
                            .getBiFieldSlicingCoercer(tagsStats.field("values", String.class), TAGS.TAGS_TYPE,
                                                      (slicer, v) -> ResourcesHelper.coerceTagValue(v, slicer.getRaw(TAGS.TAGS_TYPE)))
                            .nonConsuming()));
    }
  }
}
