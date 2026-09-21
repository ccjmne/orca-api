package org.ccjmne.orca.api.rest.admin;

import static org.ccjmne.orca.jooq.codegen.Tables.CERTIFICATES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGS;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGTYPES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGTYPES_CERTIFICATES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGTYPES_DEFS;

import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;

import org.ccjmne.orca.api.inject.business.QueryParams;
import org.ccjmne.orca.api.inject.business.Restrictions;
import org.ccjmne.orca.api.utils.Fields;
import org.ccjmne.orca.api.utils.ParamsAssertion;
import org.ccjmne.orca.api.utils.Transactions;
import org.ccjmne.orca.jooq.codegen.tables.records.TrainingtypesDefsRecord;
import org.jooq.DSLContext;
import org.jooq.Param;
import org.jooq.Row1;
import org.jooq.Row3;
import org.jooq.impl.DSL;

@Path("certificates")
public class CertificatesEndpoint {

  private final DSLContext      ctx;
  private final ParamsAssertion ensure;
  private final Param<Integer>  certificate;
  private final Param<Integer>  sessionType;

  @Inject
  public CertificatesEndpoint(final DSLContext ctx, final Restrictions restrictions, final ParamsAssertion assertor, final QueryParams parameters) {
    if (!restrictions.canManageCertificates()) {
      throw new ForbiddenException();
    }

    this.ctx = ctx;
    this.ensure = assertor;
    this.certificate = parameters.get(QueryParams.CERTIFICATE);
    this.sessionType = parameters.get(QueryParams.SESSION_TYPE);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Integer createCert(final Map<String, Object> cert) {
    return this.ctx
        .insertInto(CERTIFICATES)
        .set(CERTIFICATES.CERT_NAME, (String) cert.get(CERTIFICATES.CERT_NAME.getName()))
        .set(CERTIFICATES.CERT_SHORT, (String) cert.get(CERTIFICATES.CERT_SHORT.getName()))
        .set(CERTIFICATES.CERT_TARGET, (Integer) cert.get(CERTIFICATES.CERT_TARGET.getName()))
        .set(CERTIFICATES.CERT_ORDER, DSL.select(DSL.count().plus(DSL.one())).from(CERTIFICATES)) // TODO: does it need coalescing?
        .returning(CERTIFICATES.CERT_PK)
        .fetchOne().getValue(CERTIFICATES.CERT_PK);
  }

  @PUT
  @Path("{certificate}")
  @Consumes(MediaType.APPLICATION_JSON)
  public void updateCert(final Map<String, Object> cert) {
    Transactions.with(this.ctx, transactionCtx -> {
      this.ensure.resourceExists(QueryParams.CERTIFICATE, transactionCtx).or("No such certificate.");

      transactionCtx
          .update(CERTIFICATES)
          .set(CERTIFICATES.CERT_NAME, (String) cert.get(CERTIFICATES.CERT_NAME.getName()))
          .set(CERTIFICATES.CERT_SHORT, (String) cert.get(CERTIFICATES.CERT_SHORT.getName()))
          .set(CERTIFICATES.CERT_TARGET, (Integer) cert.get(CERTIFICATES.CERT_TARGET.getName()))
          .where(CERTIFICATES.CERT_PK.eq(this.certificate))
          .execute();
    });
  }

  @DELETE
  @Path("{certificate}")
  public void deleteCert() {
    Transactions.with(this.ctx, transactionCtx -> {
      this.ensure.resourceExists(QueryParams.CERTIFICATE, transactionCtx);

      transactionCtx.delete(CERTIFICATES).where(CERTIFICATES.CERT_PK.eq(this.certificate)).execute();
      transactionCtx.execute(Fields.cleanupSequence(CERTIFICATES, CERTIFICATES.CERT_PK, CERTIFICATES.CERT_ORDER));
    });
  }

  @POST
  @Path("reorder")
  @Consumes(MediaType.APPLICATION_JSON)
  @SuppressWarnings({ "unchecked", "null" })
  public void reorderCerts(final List<Integer> certificates) {
    if ((certificates == null) || certificates.isEmpty()) {
      return;
    }

    this.ctx
        .update(CERTIFICATES)
        .set(CERTIFICATES.CERT_ORDER, DSL.field("idx", Integer.class))
        .from(DSL.select(DSL.field("key"), DSL.rowNumber().over().as("idx"))
            .from(DSL.values(certificates.stream().map(DSL::row).toArray(Row1[]::new)).as("unused", "key")))
        .where(CERTIFICATES.CERT_PK.eq(DSL.field("key", Integer.class)))
        .execute();
  }

  // SESSION-TYPES SECTION
  // TODO: maybe extract into its own endpoint?

  @POST
  @Path("session-types")
  @Consumes(MediaType.APPLICATION_JSON)
  @SuppressWarnings("unchecked")
  public Integer createSessionType(final Map<String, Object> type) {
    return Transactions.with(this.ctx, transactionCtx -> {
      final Integer id = transactionCtx
          .insertInto(TRAININGTYPES)
          .set(TRAININGTYPES.TRTY_NAME, (String) type.get(TRAININGTYPES.TRTY_NAME.getName()))
          .set(TRAININGTYPES.TRTY_ORDER, DSL.select(DSL.count().plus(DSL.one())).from(TRAININGTYPES)) // TODO: does it need coalescing?
          .returning(TRAININGTYPES.TRTY_PK)
          .fetchOne().getValue(TRAININGTYPES.TRTY_PK);

      final Integer definition = transactionCtx
          .insertInto(TRAININGTYPES_DEFS)
          .set(TRAININGTYPES_DEFS.TTDF_TRTY_FK, id)
          .set(TRAININGTYPES_DEFS.TTDF_EFFECTIVE_FROM, Fields.DATE_NEGATIVE_INFINITY)
          .returning(TRAININGTYPES_DEFS.TTDF_PK)
          .fetchOne().getValue(TRAININGTYPES_DEFS.TTDF_PK);

      CertificatesEndpoint.replaceDefinition(transactionCtx, definition, type);

      return id;
    });
  }

  @PUT
  @Path("session-types/{session-type}")
  @Consumes(MediaType.APPLICATION_JSON)
  public void updateSessionType(final Map<String, Object> type) {
    Transactions.with(this.ctx, transactionCtx -> {
      this.ensure.resourceExists(QueryParams.SESSION_TYPE, transactionCtx);

      transactionCtx
          .update(TRAININGTYPES)
          .set(TRAININGTYPES.TRTY_NAME, (String) type.get(TRAININGTYPES.TRTY_NAME.getName()))
          .where(TRAININGTYPES.TRTY_PK.eq(this.sessionType))
          .execute();
    });
  }

  @POST
  @Path("session-types/{session-type}/definitions")
  @Consumes(MediaType.APPLICATION_JSON)
  public Integer createDefinition(final Map<String, Object> definition) {
    return Transactions.with(this.ctx, transactionCtx -> {
      final Integer type = this.sessionType.getValue();
      final LocalDate effectiveFrom = LocalDate.parse((String) definition.get(TRAININGTYPES_DEFS.TTDF_EFFECTIVE_FROM.getName()));
      final Integer id = transactionCtx.insertInto(TRAININGTYPES_DEFS)
          .set(TRAININGTYPES_DEFS.TTDF_TRTY_FK, type)
          .set(TRAININGTYPES_DEFS.TTDF_EFFECTIVE_FROM, effectiveFrom)
          .set(TRAININGTYPES_DEFS.TTDF_PRESENCEONLY, (Boolean) definition.get(TRAININGTYPES_DEFS.TTDF_PRESENCEONLY.getName()))
          .set(TRAININGTYPES_DEFS.TTDF_EXTENDVALIDITY, (Boolean) definition.get(TRAININGTYPES_DEFS.TTDF_EXTENDVALIDITY.getName()))
          .set(TRAININGTYPES_DEFS.TTDF_CERTIFIED, (Boolean) definition.get(TRAININGTYPES_DEFS.TTDF_CERTIFIED.getName()))
          .returning(TRAININGTYPES_DEFS.TTDF_PK)
          .fetchOne().getValue(TRAININGTYPES_DEFS.TTDF_PK);
      if (CertificatesEndpoint.definitionUsedBySession(transactionCtx, type, id)) {
        throw new IllegalArgumentException("The new definition would change existing session history.");
      }
      CertificatesEndpoint.replaceDefinition(transactionCtx, id, definition);
      return id;
    });
  }

  @PUT
  @Path("session-types/{session-type}/definitions/{definition}")
  @Consumes(MediaType.APPLICATION_JSON)
  public void updateDefinition(@PathParam("definition") final Integer definitionId, final Map<String, Object> definition) {
    Transactions.with(this.ctx, transactionCtx -> {
      final TrainingtypesDefsRecord existing = CertificatesEndpoint.requireDefinition(transactionCtx, this.sessionType.getValue(), definitionId);
      if (CertificatesEndpoint.definitionUsedBySession(transactionCtx, existing.getTtdfTrtyFk(), definitionId)) {
        throw new IllegalArgumentException("A definition used by existing sessions cannot be changed.");
      }
      CertificatesEndpoint.replaceDefinition(transactionCtx, definitionId, definition);
    });
  }

  @DELETE
  @Path("session-types/{session-type}/definitions/{definition}")
  public void deleteDefinition(@PathParam("definition") final Integer definitionId) {
    Transactions.with(this.ctx, transactionCtx -> {
      final TrainingtypesDefsRecord definition = CertificatesEndpoint.requireDefinition(transactionCtx, this.sessionType.getValue(), definitionId);
      if (transactionCtx.fetchExists(TRAININGTYPES_DEFS,
                                     TRAININGTYPES_DEFS.TTDF_PK.eq(definitionId)
                                         .and(TRAININGTYPES_DEFS.TTDF_EFFECTIVE_FROM.eq(Fields.DATE_NEGATIVE_INFINITY)))) {
        throw new IllegalArgumentException("The baseline definition cannot be deleted.");
      }
      if (CertificatesEndpoint.definitionUsedBySession(transactionCtx, definition.getTtdfTrtyFk(), definitionId)) {
        throw new IllegalArgumentException("A definition used by existing sessions cannot be deleted.");
      }
      transactionCtx.deleteFrom(TRAININGTYPES_DEFS)
          .where(TRAININGTYPES_DEFS.TTDF_PK.eq(definitionId))
          .execute();
    });
  }

  @DELETE
  @Path("session-types/{session-type}")
  public void deleteTrty() {
    Transactions.with(this.ctx, transactionCtx -> {
      this.ensure.resourceExists(QueryParams.SESSION_TYPE, transactionCtx);
      if (transactionCtx.fetchExists(TRAININGS, TRAININGS.TRNG_TRTY_FK.eq(this.sessionType))) {
        throw new IllegalArgumentException("A session type with existing sessions cannot be deleted.");
      }

      transactionCtx.delete(TRAININGTYPES).where(TRAININGTYPES.TRTY_PK.eq(this.sessionType)).execute();
      transactionCtx.execute(Fields.cleanupSequence(TRAININGTYPES, TRAININGTYPES.TRTY_PK, TRAININGTYPES.TRTY_ORDER));
    });
  }

  @POST
  @Path("session-types/reorder")
  @Consumes(MediaType.APPLICATION_JSON)
  @SuppressWarnings({ "unchecked", "null" })
  public void reorderTypes(final List<Integer> sessionTypes) {
    if ((null == sessionTypes) || sessionTypes.isEmpty()) {
      return;
    }

    this.ctx
        .update(TRAININGTYPES)
        .set(TRAININGTYPES.TRTY_ORDER, DSL.field("idx", Integer.class))
        .from(DSL.select(DSL.field("key"), DSL.rowNumber().over().as("idx"))
            .from(DSL.values(sessionTypes.stream().map(DSL::row).toArray(Row1[]::new)).as("unused", "key")))
        .where(TRAININGTYPES.TRTY_PK.eq(DSL.field("key", Integer.class)))
        .execute();
  }

  @SuppressWarnings("unchecked")
  private static void replaceDefinition(final DSLContext transactionCtx, final Integer id, final Map<String, Object> definition) {
    final List<Map<String, Integer>> certificateList = (List<Map<String, Integer>>) definition.getOrDefault("certificates", Collections.EMPTY_LIST);
    if (certificateList.stream().anyMatch(certificate -> certificate.get(TRAININGTYPES_CERTIFICATES.TTCE_DURATION.getName()).intValue() < 0)) {
      throw new IllegalArgumentException("Certificate durations cannot be negative.");
    }
    transactionCtx.update(TRAININGTYPES_DEFS)
        .set(TRAININGTYPES_DEFS.TTDF_PRESENCEONLY,
             DSL.coalesce(DSL.val((Boolean) definition.get(TRAININGTYPES_DEFS.TTDF_PRESENCEONLY.getName())), TRAININGTYPES_DEFS.TTDF_PRESENCEONLY))
        .set(TRAININGTYPES_DEFS.TTDF_EXTENDVALIDITY,
             DSL.coalesce(DSL.val((Boolean) definition.get(TRAININGTYPES_DEFS.TTDF_EXTENDVALIDITY.getName())), TRAININGTYPES_DEFS.TTDF_EXTENDVALIDITY))
        .set(TRAININGTYPES_DEFS.TTDF_CERTIFIED,
             DSL.coalesce(DSL.val((Boolean) definition.get(TRAININGTYPES_DEFS.TTDF_CERTIFIED.getName())), TRAININGTYPES_DEFS.TTDF_CERTIFIED))
        .where(TRAININGTYPES_DEFS.TTDF_PK.eq(id))
        .execute();
    transactionCtx.deleteFrom(TRAININGTYPES_CERTIFICATES)
        .where(TRAININGTYPES_CERTIFICATES.TTCE_TTDF_FK.eq(id))
        .execute();

    final Row3<Integer, Integer, Integer>[] certificates = certificateList.stream()
        .map(certificate -> DSL.row(id,
                                    certificate.get(CERTIFICATES.CERT_PK.getName()),
                                    certificate.get(TRAININGTYPES_CERTIFICATES.TTCE_DURATION.getName())))
        .toArray(Row3[]::new);
    if (certificates.length > 0) {
      transactionCtx.insertInto(
                                TRAININGTYPES_CERTIFICATES,
                                TRAININGTYPES_CERTIFICATES.TTCE_TTDF_FK,
                                TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK,
                                TRAININGTYPES_CERTIFICATES.TTCE_DURATION)
          .select(DSL.selectFrom(DSL.values(certificates).as(DSL.table(),
                                                            TRAININGTYPES_CERTIFICATES.TTCE_TTDF_FK,
                                                            TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK,
                                                            TRAININGTYPES_CERTIFICATES.TTCE_DURATION)))
          .execute();
    }
  }

  private static TrainingtypesDefsRecord requireDefinition(final DSLContext transactionCtx, final Integer type, final Integer definition) {
    return transactionCtx.selectFrom(TRAININGTYPES_DEFS)
        .where(TRAININGTYPES_DEFS.TTDF_PK.eq(definition))
        .and(TRAININGTYPES_DEFS.TTDF_TRTY_FK.eq(type))
        .fetchOptional()
        .orElseThrow(() -> new org.jooq.exception.NoDataFoundException("No such definition for that session type."));
  }

  private static boolean definitionUsedBySession(final DSLContext transactionCtx, final Integer type, final Integer definition) {
    return transactionCtx.fetchExists(transactionCtx.selectOne()
        .from(TRAININGS)
        .where(TRAININGS.TRNG_TRTY_FK.eq(type))
        .and(Fields.selectTypeDefinition(TRAININGS.TRNG_TRTY_FK, TRAININGS.TRNG_DATE).eq(definition)));
  }

}
