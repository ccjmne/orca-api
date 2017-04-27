package org.ccjmne.orca.api.rest;

import static org.ccjmne.orca.jooq.classes.Tables.CERTIFICATES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAINERPROFILES_TRAININGTYPES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGTYPES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGTYPES_CERTIFICATES;

import java.util.ArrayList;
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

import org.ccjmne.orca.jooq.classes.Sequences;
import org.ccjmne.orca.api.modules.Restrictions;
import org.ccjmne.orca.api.utils.Constants;
import org.jooq.DSLContext;
import org.jooq.Row1;
import org.jooq.Row2;
import org.jooq.impl.DSL;

@Path("certificates")
public class CertificatesEndpoint {

	private final DSLContext ctx;

	@Inject
	public CertificatesEndpoint(final DSLContext ctx, final Restrictions restrictions) {
		if (!restrictions.canManageCertificates()) {
			throw new ForbiddenException();
		}

		this.ctx = ctx;
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	public Integer createCert(final Map<String, String> cert) {
		final Integer cert_pk = new Integer(this.ctx.nextval(Sequences.CERTIFICATES_CERT_PK_SEQ).intValue());
		updateCert(cert_pk, cert);
		return cert_pk;
	}

	@PUT
	@Path("{cert_pk}")
	@Consumes(MediaType.APPLICATION_JSON)
	public boolean updateCert(@PathParam("cert_pk") final Integer cert_pk, final Map<String, String> cert) {
		final boolean exists = this.ctx.fetchExists(CERTIFICATES, CERTIFICATES.CERT_PK.eq(cert_pk));
		this.ctx.transaction((config) -> {
			try (final DSLContext transactionCtx = DSL.using(config)) {
				if (exists) {
					transactionCtx.update(CERTIFICATES)
							.set(CERTIFICATES.CERT_NAME, cert.get(CERTIFICATES.CERT_NAME.getName()))
							.set(CERTIFICATES.CERT_SHORT, cert.get(CERTIFICATES.CERT_SHORT.getName()))
							.set(CERTIFICATES.CERT_TARGET, Integer.valueOf(cert.get(CERTIFICATES.CERT_TARGET.getName())))
							.set(CERTIFICATES.CERT_PERMANENTONLY, Boolean.valueOf(cert.get(CERTIFICATES.CERT_PERMANENTONLY.getName())))
							.where(CERTIFICATES.CERT_PK.eq(cert_pk)).execute();
				} else {
					transactionCtx.insertInto(
												CERTIFICATES,
												CERTIFICATES.CERT_PK,
												CERTIFICATES.CERT_NAME,
												CERTIFICATES.CERT_SHORT,
												CERTIFICATES.CERT_TARGET,
												CERTIFICATES.CERT_PERMANENTONLY,
												CERTIFICATES.CERT_ORDER)
							.values(
									cert_pk,
									cert.get(CERTIFICATES.CERT_NAME.getName()),
									cert.get(CERTIFICATES.CERT_SHORT.getName()),
									Integer.valueOf(cert.get(CERTIFICATES.CERT_TARGET.getName())),
									Boolean.valueOf(cert.get(CERTIFICATES.CERT_PERMANENTONLY.getName())),
									transactionCtx
											.select(DSL.coalesce(DSL.max(CERTIFICATES.CERT_ORDER), Integer.valueOf(0)).add(Integer.valueOf(1)).as("order"))
											.from(CERTIFICATES)
											.fetchOne("order", Integer.class))
							.execute();
				}
			}
		});

		return exists;
	}

	@POST
	@Path("trainingtypes")
	@Consumes(MediaType.APPLICATION_JSON)
	public Integer createTrty(final Map<String, Object> trty) {
		final Integer trty_pk = new Integer(this.ctx.nextval(Sequences.TRAININGTYPES_TRTY_PK_SEQ).intValue());
		updateTrty(trty_pk, trty);
		return trty_pk;
	}

	@SuppressWarnings("unchecked")
	@PUT
	@Path("trainingtypes/{trty_pk}")
	@Consumes(MediaType.APPLICATION_JSON)
	public boolean updateTrty(@PathParam("trty_pk") final Integer trty_pk, final Map<String, Object> trty) {
		final boolean exists = this.ctx.fetchExists(TRAININGTYPES, TRAININGTYPES.TRTY_PK.eq(trty_pk));
		this.ctx.transaction((config) -> {
			try (final DSLContext transactionCtx = DSL.using(config)) {
				if (exists) {
					transactionCtx.update(TRAININGTYPES)
							.set(TRAININGTYPES.TRTY_PK, trty_pk)
							.set(TRAININGTYPES.TRTY_NAME, trty.get(TRAININGTYPES.TRTY_NAME.getName()).toString())
							.set(TRAININGTYPES.TRTY_VALIDITY, Integer.valueOf(trty.get(TRAININGTYPES.TRTY_VALIDITY.getName()).toString()))
							.where(TRAININGTYPES.TRTY_PK.eq(trty_pk)).execute();
				} else {
					transactionCtx.insertInto(
												TRAININGTYPES,
												TRAININGTYPES.TRTY_PK,
												TRAININGTYPES.TRTY_NAME,
												TRAININGTYPES.TRTY_VALIDITY,
												TRAININGTYPES.TRTY_ORDER)
							.values(
									trty_pk,
									trty.get(TRAININGTYPES.TRTY_NAME.getName()).toString(),
									(Integer) trty.get(TRAININGTYPES.TRTY_VALIDITY.getName()),
									transactionCtx
											.select(DSL.coalesce(DSL.max(TRAININGTYPES.TRTY_ORDER), Integer.valueOf(0)).add(Integer.valueOf(1)).as("order"))
											.from(TRAININGTYPES)
											.fetchOne("order", Integer.class))
							.execute();

					transactionCtx
							.insertInto(TRAINERPROFILES_TRAININGTYPES, TRAINERPROFILES_TRAININGTYPES.TPTT_TRPR_FK, TRAINERPROFILES_TRAININGTYPES.TPTT_TRTY_FK)
							.values(Constants.UNASSIGNED_TRAINERPROFILE, trty_pk).execute();
				}

				transactionCtx.delete(TRAININGTYPES_CERTIFICATES).where(TRAININGTYPES_CERTIFICATES.TTCE_TRTY_FK.eq(trty_pk)).execute();
				final Row1<Integer>[] certificates = ((List<Integer>) trty.get("certificates")).stream().map(DSL::row).toArray(Row1[]::new);
				if (certificates.length > 0) {
					transactionCtx.insertInto(
												TRAININGTYPES_CERTIFICATES,
												TRAININGTYPES_CERTIFICATES.TTCE_TRTY_FK,
												TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK)
							.select(DSL.select(
												DSL.val(trty_pk),
												DSL.field("cert_pk", Integer.class))
									.from(DSL.values(certificates).as("unused", "cert_pk")))
							.execute();
				}
			}
		});

		return exists;
	}

	@POST
	@SuppressWarnings({ "unchecked", "null" })
	@Path("reorder")
	public void reassignCertificates(final Map<Integer, Integer> reassignmentMap) {
		if (reassignmentMap.isEmpty()) {
			return;
		}

		final List<Row2<Integer, Integer>> rows = new ArrayList<>(reassignmentMap.size());
		reassignmentMap.entrySet().forEach(entry -> rows.add(DSL.row(entry.getKey(), entry.getValue())));
		this.ctx.update(CERTIFICATES)
				.set(CERTIFICATES.CERT_ORDER, DSL.field("new_order", Integer.class))
				.from(DSL.values(rows.toArray(new Row2[0])).as("unused", "pk", "new_order"))
				.where(CERTIFICATES.CERT_PK.eq(DSL.field("pk", Integer.class)))
				.execute();
	}

	@POST
	@SuppressWarnings("unchecked")
	@Path("trainingtypes/reorder")
	public void reassignTrainingTypes(final Map<Integer, Integer> reassignmentMap) {
		if (reassignmentMap.isEmpty()) {
			return;
		}

		final List<Row2<Integer, Integer>> rows = new ArrayList<>(reassignmentMap.size());
		reassignmentMap.entrySet().forEach(entry -> rows.add(DSL.row(entry.getKey(), entry.getValue())));
		this.ctx.update(TRAININGTYPES)
				.set(TRAININGTYPES.TRTY_ORDER, DSL.field("new_order", Integer.class))
				.from(DSL.values(rows.toArray(new Row2[0])).as("unused", "pk", "new_order"))
				.where(TRAININGTYPES.TRTY_PK.eq(DSL.field("pk", Integer.class)))
				.execute();
	}

	@DELETE
	@Path("{cert_pk}")
	public boolean deleteCert(@PathParam("cert_pk") final Integer cert_pk) {
		return this.ctx.delete(CERTIFICATES).where(CERTIFICATES.CERT_PK.eq(cert_pk)).execute() == 1;
	}

	@DELETE
	@Path("trainingtypes/{trty_pk}")
	public boolean deleteTrty(@PathParam("trty_pk") final Integer trty_pk) {
		return this.ctx.delete(TRAININGTYPES).where(TRAININGTYPES.TRTY_PK.eq(trty_pk)).execute() == 1;
	}
}
