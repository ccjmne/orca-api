package org.ccjmne.orca.api.rest.admin;

import static org.ccjmne.orca.jooq.codegen.Tables.CERTIFICATES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGTYPES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGTYPES_CERTIFICATES;

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
import javax.ws.rs.core.MediaType;

import org.ccjmne.orca.api.inject.business.QueryParams;
import org.ccjmne.orca.api.inject.business.Restrictions;
import org.ccjmne.orca.api.utils.Fields;
import org.ccjmne.orca.api.utils.ParamsAssertion;
import org.ccjmne.orca.api.utils.Transactions;
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
        .set(CERTIFICATES.CERT_ORDER, DSL.select(DSL.count().plus(Integer.valueOf(1))).from(CERTIFICATES)) // TODO: does it need coalescing?
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
          .set(TRAININGTYPES.TRTY_ORDER, DSL.select(DSL.count().plus(Integer.valueOf(1))).from(TRAININGTYPES)) // TODO: does it need coalescing?
          .returning(TRAININGTYPES.TRTY_PK)
          .fetchOne().getValue(TRAININGTYPES.TRTY_PK);

      final Row3<Integer, Integer, Integer>[] certs = ((List<Map<String, Integer>>) type.getOrDefault("certificates", Collections.EMPTY_LIST)).stream()
          .map(c -> DSL.row(id, c.get(CERTIFICATES.CERT_PK.getName()), c.get(TRAININGTYPES_CERTIFICATES.TTCE_DURATION.getName())))
          .toArray(Row3[]::new);

      if (certs.length > 0) {
        transactionCtx
            .insertInto(TRAININGTYPES_CERTIFICATES)
            .select(DSL.selectFrom(DSL.values(certs).as(DSL.table(),
                                                        TRAININGTYPES_CERTIFICATES.TTCE_TRTY_FK,
                                                        TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK,
                                                        TRAININGTYPES_CERTIFICATES.TTCE_DURATION)))
            .execute();
      }

      return id;
    });
  }

  @PUT
  @Path("session-types/{session-type}")
  @Consumes(MediaType.APPLICATION_JSON)
  @SuppressWarnings("unchecked")
  public void updateSessionType(final Map<String, Object> type) {
    Transactions.with(this.ctx, transactionCtx -> {
      this.ensure.resourceExists(QueryParams.SESSION_TYPE, transactionCtx);

      transactionCtx
          .update(TRAININGTYPES)
          .set(TRAININGTYPES.TRTY_NAME, (String) type.get(TRAININGTYPES.TRTY_NAME.getName()))
          .where(TRAININGTYPES.TRTY_PK.eq(this.sessionType))
          .execute();

      transactionCtx.delete(TRAININGTYPES_CERTIFICATES).where(TRAININGTYPES_CERTIFICATES.TTCE_TRTY_FK.eq(this.sessionType)).execute();

      final Row3<Integer, Integer, Integer>[] certs = ((List<Map<String, Integer>>) type.getOrDefault("certificates", Collections.EMPTY_LIST)).stream()
          .map(c -> DSL.row(this.sessionType, c.get(CERTIFICATES.CERT_PK.getName()), c.get(TRAININGTYPES_CERTIFICATES.TTCE_DURATION.getName())))
          .toArray(Row3[]::new);

      if (certs.length > 0) {
        transactionCtx
            .insertInto(TRAININGTYPES_CERTIFICATES)
            .select(DSL.selectFrom(DSL.values(certs).as(DSL.table(),
                                                        TRAININGTYPES_CERTIFICATES.TTCE_TRTY_FK,
                                                        TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK,
                                                        TRAININGTYPES_CERTIFICATES.TTCE_DURATION)))
            .execute();
      }
    });
  }

  @DELETE
  @Path("session-types/{session-type}")
  public void deleteTrty() {
    Transactions.with(this.ctx, transactionCtx -> {
      this.ensure.resourceExists(QueryParams.SESSION_TYPE, transactionCtx);

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
}
