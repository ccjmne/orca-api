package org.ccjmne.orca.api.rest.admin;

import static org.ccjmne.orca.jooq.codegen.Tables.CERTIFICATES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGTYPES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGTYPES_CERTIFICATES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGTYPES_DEFS;

import java.time.LocalDate;
import java.util.ArrayList;
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
import org.jooq.DSLContext;
import org.jooq.Param;
import org.jooq.Row1;
import org.jooq.Row3;
import org.jooq.exception.NoDataFoundException;
import org.jooq.impl.DSL;

@Path("certificates")
public class CertificatesEndpoint {

  private final DSLContext      ctx;
  private final ParamsAssertion ensure;
  private final Param<Integer>  cert;
  private final Param<Integer>  trty;

  @Inject
  public CertificatesEndpoint(final DSLContext ctx, final Restrictions restrictions, final ParamsAssertion ensure, final QueryParams params) {
    if (!restrictions.canManageCertificates()) throw new ForbiddenException();
    this.ctx    = ctx;
    this.ensure = ensure;
    this.cert   = params.get(QueryParams.CERTIFICATE);
    this.trty   = params.get(QueryParams.SESSION_TYPE);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Integer createCert(final Map<String, Object> cert) {
    return this.ctx
        .insertInto(CERTIFICATES)
        .set(CERTIFICATES.CERT_NAME,   (String)  cert.get(CERTIFICATES.CERT_NAME.getName()))
        .set(CERTIFICATES.CERT_SHORT,  (String)  cert.get(CERTIFICATES.CERT_SHORT.getName()))
        .set(CERTIFICATES.CERT_TARGET, (Integer) cert.get(CERTIFICATES.CERT_TARGET.getName()))
        .set(CERTIFICATES.CERT_ORDER,  DSL.select(DSL.coalesce(DSL.max(CERTIFICATES.CERT_ORDER), DSL.zero()).plus(DSL.one())).from(CERTIFICATES))
        .returning(CERTIFICATES.CERT_PK)
        .fetchOne().getValue(CERTIFICATES.CERT_PK);
  }

  @PUT
  @Path("{certificate}")
  @Consumes(MediaType.APPLICATION_JSON)
  public void updateCert(final Map<String, Object> cert) {
    this.ensure.resourceExists(QueryParams.CERTIFICATE, this.ctx);
    this.ctx.update(CERTIFICATES)
        .set(CERTIFICATES.CERT_NAME,   (String)  cert.get(CERTIFICATES.CERT_NAME.getName()))
        .set(CERTIFICATES.CERT_SHORT,  (String)  cert.get(CERTIFICATES.CERT_SHORT.getName()))
        .set(CERTIFICATES.CERT_TARGET, (Integer) cert.get(CERTIFICATES.CERT_TARGET.getName()))
        .where(CERTIFICATES.CERT_PK.eq(this.cert))
        .execute();
  }

  @DELETE
  @Path("{certificate}")
  public void deleteCert() {
    Transactions.with(this.ctx, tx -> {
      this.ensure.resourceExists(QueryParams.CERTIFICATE, tx);
      tx.delete(CERTIFICATES).where(CERTIFICATES.CERT_PK.eq(this.cert)).execute();
      tx.execute(Fields.cleanupSequence(CERTIFICATES, CERTIFICATES.CERT_PK, CERTIFICATES.CERT_ORDER));
    });
  }

  @POST
  @Path("reorder")
  @Consumes(MediaType.APPLICATION_JSON)
  @SuppressWarnings({ "unchecked" })
  public void reorderCerts(final List<Integer> certs) {
    if (null == certs || certs.isEmpty()) return;
    this.ctx
        .update(CERTIFICATES)
        .set(CERTIFICATES.CERT_ORDER, DSL.field("idx", Integer.class))
        .from(DSL.select(DSL.field("key"), DSL.rowNumber().over().as("idx"))
            .from(DSL.values(certs.stream().map(DSL::row).toArray(Row1[]::new)).as("unused", "key")))
        .where(CERTIFICATES.CERT_PK.eq(DSL.field("key", Integer.class)))
        .execute();
  }

  // SESSION-TYPES SECTION
  // TODO: maybe extract into its own endpoint?

  @POST
  @Path("session-types")
  @Consumes(MediaType.APPLICATION_JSON)
  @SuppressWarnings("unchecked")
  public Integer createTrty(final Map<String, Object> trty) {
    return Transactions.with(this.ctx, tx -> {
      final var id = tx
          .insertInto(TRAININGTYPES)
          .set(TRAININGTYPES.TRTY_NAME, (String) trty.get(TRAININGTYPES.TRTY_NAME.getName()))
          .set(TRAININGTYPES.TRTY_ORDER, DSL.select(DSL.coalesce(DSL.max(TRAININGTYPES.TRTY_ORDER), DSL.zero()).plus(DSL.one())).from(TRAININGTYPES))
          .returning(TRAININGTYPES.TRTY_PK)
          .fetchOne().getValue(TRAININGTYPES.TRTY_PK);

      final var ttdf = tx
          .insertInto(TRAININGTYPES_DEFS)
          .set(TRAININGTYPES_DEFS.TTDF_TRTY_FK, id)
          .set(TRAININGTYPES_DEFS.TTDF_EFFECTIVE_FROM, Fields.DATE_NEGATIVE_INFINITY)
          .set(TRAININGTYPES_DEFS.TTDF_PRESENCEONLY,   (Boolean) trty.get(TRAININGTYPES_DEFS.TTDF_PRESENCEONLY.getName()))
          .set(TRAININGTYPES_DEFS.TTDF_EXTENDVALIDITY, (Boolean) trty.get(TRAININGTYPES_DEFS.TTDF_EXTENDVALIDITY.getName()))
          .set(TRAININGTYPES_DEFS.TTDF_CERTIFIED,      (Boolean) trty.get(TRAININGTYPES_DEFS.TTDF_CERTIFIED.getName()))
          .returning(TRAININGTYPES_DEFS.TTDF_PK)
          .fetchOne().getValue(TRAININGTYPES_DEFS.TTDF_PK);

      if (trty.containsKey(CERTIFICATES.CERT_SHORT.getName())) {
        final var cert = tx
            .insertInto(CERTIFICATES)
            .set(CERTIFICATES.CERT_NAME,    (String)  trty.get(CERTIFICATES.CERT_NAME.getName()))
            .set(CERTIFICATES.CERT_SHORT,   (String)  trty.get(CERTIFICATES.CERT_SHORT.getName()))
            .set(CERTIFICATES.CERT_TARGET,  (Integer) trty.get(CERTIFICATES.CERT_TARGET.getName()))
            .set(CERTIFICATES.CERT_ORDER,   DSL.select(DSL.coalesce(DSL.max(CERTIFICATES.CERT_ORDER), DSL.zero()).plus(DSL.one())).from(CERTIFICATES))
            .set(CERTIFICATES.CERT_TRTY_FK, id)
            .returning(CERTIFICATES.CERT_PK)
            .fetchOne().getValue(CERTIFICATES.CERT_PK);
        ((List<Map<String, Integer>>) trty.computeIfAbsent("certificates", key -> new ArrayList<>()))
            .add(Map.of(CERTIFICATES.CERT_PK.getName(), cert,
                    TRAININGTYPES_CERTIFICATES.TTCE_DURATION.getName(),
                    (Integer) trty.get(TRAININGTYPES_CERTIFICATES.TTCE_DURATION.getName())));
      }

      CertificatesEndpoint.replaceTtce(tx, ttdf, id, trty);
      return id;
    });
  }

  @PUT
  @Path("session-types/{session-type}")
  @Consumes(MediaType.APPLICATION_JSON)
  public void updateTrty(final Map<String, Object> trty) {
    this.ensure.resourceExists(QueryParams.SESSION_TYPE, this.ctx);
    this.ctx.update(TRAININGTYPES)
        .set(TRAININGTYPES.TRTY_NAME, (String) trty.get(TRAININGTYPES.TRTY_NAME.getName()))
        .where(TRAININGTYPES.TRTY_PK.eq(this.trty))
        .execute();
  }

  @POST
  @Path("session-types/{session-type}/definitions")
  @Consumes(MediaType.APPLICATION_JSON)
  public Integer createTtdf(final Map<String, Object> ttdf) {
    return Transactions.with(this.ctx, tx -> {
      final var trty = this.trty.getValue();
      final var id = tx.insertInto(TRAININGTYPES_DEFS)
          .set(TRAININGTYPES_DEFS.TTDF_TRTY_FK, trty)
          .set(TRAININGTYPES_DEFS.TTDF_EFFECTIVE_FROM,
               DSL.field("{0}::date", LocalDate.class, (String)  ttdf.get(TRAININGTYPES_DEFS.TTDF_EFFECTIVE_FROM.getName())))
          .set(TRAININGTYPES_DEFS.TTDF_PRESENCEONLY,   (Boolean) ttdf.get(TRAININGTYPES_DEFS.TTDF_PRESENCEONLY.getName()))
          .set(TRAININGTYPES_DEFS.TTDF_EXTENDVALIDITY, (Boolean) ttdf.get(TRAININGTYPES_DEFS.TTDF_EXTENDVALIDITY.getName()))
          .set(TRAININGTYPES_DEFS.TTDF_CERTIFIED,      (Boolean) ttdf.get(TRAININGTYPES_DEFS.TTDF_CERTIFIED.getName()))
          .returning(TRAININGTYPES_DEFS.TTDF_PK)
          .fetchOne().getValue(TRAININGTYPES_DEFS.TTDF_PK);
      CertificatesEndpoint.replaceTtce(tx, id, trty, ttdf);
      return id;
    });
  }

  @PUT
  @Path("session-types/{session-type}/definitions/{definition}")
  @Consumes(MediaType.APPLICATION_JSON)
  public void updateTtdf(@PathParam("definition") final Integer id, final Map<String, Object> ttdf) {
    Transactions.with(this.ctx, tx -> {
      if (tx.update(TRAININGTYPES_DEFS)
          .set(TRAININGTYPES_DEFS.TTDF_EFFECTIVE_FROM,
               DSL.field("{0}::date", LocalDate.class, (String)  ttdf.get(TRAININGTYPES_DEFS.TTDF_EFFECTIVE_FROM.getName())))
          .set(TRAININGTYPES_DEFS.TTDF_PRESENCEONLY,   (Boolean) ttdf.get(TRAININGTYPES_DEFS.TTDF_PRESENCEONLY.getName()))
          .set(TRAININGTYPES_DEFS.TTDF_EXTENDVALIDITY, (Boolean) ttdf.get(TRAININGTYPES_DEFS.TTDF_EXTENDVALIDITY.getName()))
          .set(TRAININGTYPES_DEFS.TTDF_CERTIFIED,      (Boolean) ttdf.get(TRAININGTYPES_DEFS.TTDF_CERTIFIED.getName()))
          .where(TRAININGTYPES_DEFS.TTDF_PK.eq(id))
          .and(TRAININGTYPES_DEFS.TTDF_TRTY_FK.eq(this.trty))
          .execute() != 1) throw new NoDataFoundException("No such definition for that training type.");
      CertificatesEndpoint.replaceTtce(tx, id, this.trty.getValue(), ttdf);
    });
  }

  @DELETE
  @Path("session-types/{session-type}/definitions/{definition}")
  public void deleteTtdf(@PathParam("definition") final Integer id) {
    if (this.ctx.deleteFrom(TRAININGTYPES_DEFS)
        .where(TRAININGTYPES_DEFS.TTDF_PK.eq(id))
        .and(TRAININGTYPES_DEFS.TTDF_TRTY_FK.eq(this.trty))
        .execute() != 1) throw new NoDataFoundException("No such definition for that training type.");
  }

  @DELETE
  @Path("session-types/{session-type}")
  public void deleteTrty() {
    Transactions.with(this.ctx, tx -> {
      this.ensure.resourceExists(QueryParams.SESSION_TYPE, tx);
      tx.delete(TRAININGTYPES).where(TRAININGTYPES.TRTY_PK.eq(this.trty)).execute();
      tx.execute(Fields.cleanupSequence(TRAININGTYPES, TRAININGTYPES.TRTY_PK, TRAININGTYPES.TRTY_ORDER));
      tx.execute(Fields.cleanupSequence(CERTIFICATES, CERTIFICATES.CERT_PK, CERTIFICATES.CERT_ORDER));
    });
  }

  @POST
  @Path("session-types/reorder")
  @Consumes(MediaType.APPLICATION_JSON)
  @SuppressWarnings({ "unchecked" })
  public void reorderTrty(final List<Integer> trty) {
    if (null == trty || trty.isEmpty()) return;
    this.ctx
        .update(TRAININGTYPES)
        .set(TRAININGTYPES.TRTY_ORDER, DSL.field("idx", Integer.class))
        .from(DSL.select(DSL.field("key"), DSL.rowNumber().over().as("idx"))
            .from(DSL.values(trty.stream().map(DSL::row).toArray(Row1[]::new)).as("unused", "key")))
        .where(TRAININGTYPES.TRTY_PK.eq(DSL.field("key", Integer.class)))
        .execute();
  }

  @SuppressWarnings("unchecked")
  private static void replaceTtce(final DSLContext tx, final Integer ttdf, final Integer trty, final Map<String, Object> data) {
    final var certs = (List<Map<String, Integer>>) data.getOrDefault("certificates", Collections.EMPTY_LIST);
    if (certs.stream().anyMatch(cert -> cert.get(TRAININGTYPES_CERTIFICATES.TTCE_DURATION.getName()).intValue() < 0)) {
      throw new IllegalArgumentException("Certificate durations cannot be negative.");
    }

    tx.deleteFrom(TRAININGTYPES_CERTIFICATES)
        .where(TRAININGTYPES_CERTIFICATES.TTCE_TTDF_FK.eq(ttdf))
        .execute();

    final var rows = certs.stream()
        .map(cert -> DSL.row(ttdf, cert.get(CERTIFICATES.CERT_PK.getName()), cert.get(TRAININGTYPES_CERTIFICATES.TTCE_DURATION.getName())))
        .toArray(Row3[]::new);
    if (rows.length > 0) {
      final var vals = DSL.values(rows).as("vals", "def", "cert", "duration");
      if (rows.length != tx.insertInto(
              TRAININGTYPES_CERTIFICATES,
              TRAININGTYPES_CERTIFICATES.TTCE_TTDF_FK,
              TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK,
              TRAININGTYPES_CERTIFICATES.TTCE_DURATION)
          .select(DSL.select(vals.field("def", Integer.class), vals.field("cert", Integer.class), vals.field("duration", Integer.class))
              .from(vals)
              .join(CERTIFICATES).on(CERTIFICATES.CERT_PK.eq(vals.field("cert", Integer.class)))
              .where(CERTIFICATES.CERT_TRTY_FK.isNull().or(CERTIFICATES.CERT_TRTY_FK.eq(trty))))
          .execute()) {
        throw new IllegalArgumentException("A certificate cannot be associated with this training type.");
      }
    }
  }
}
