package org.ccjmne.orca.api.rest.admin;

import static org.ccjmne.orca.jooq.codegen.Tables.CERTIFICATES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAINERPROFILES_TRAININGTYPES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGS;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGTYPES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGTYPES_CERTIFICATES;

import java.util.Collections;
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

import org.ccjmne.orca.api.modules.Restrictions;
import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.api.utils.ResourcesHelper;
import org.ccjmne.orca.api.utils.Transactions;
import org.ccjmne.orca.jooq.codegen.Sequences;
import org.jooq.DSLContext;
import org.jooq.Record;
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
		this.updateCert(cert_pk, cert);
		return cert_pk;
	}

	/**
	 * @return <code>true</code> iff a new {@link Record} was created
	 */
	@PUT
	@Path("{cert_pk}")
	@Consumes(MediaType.APPLICATION_JSON)
	public Boolean updateCert(@PathParam("cert_pk") final Integer cert_pk, final Map<String, String> cert) {
		return Transactions.with(this.ctx, transactionCtx -> {
			final boolean exists = this.ctx.fetchExists(CERTIFICATES, CERTIFICATES.CERT_PK.eq(cert_pk));
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

			return Boolean.valueOf(!exists);
		});
	}

	@POST
	@Path("trainingtypes")
	@Consumes(MediaType.APPLICATION_JSON)
	public Integer createTrty(final Map<String, Object> trty) {
		final Integer trty_pk = new Integer(this.ctx.nextval(Sequences.TRAININGTYPES_TRTY_PK_SEQ).intValue());
		this.updateTrty(trty_pk, trty);
		return trty_pk;
	}

	/**
	 * @return <code>true</code> iff a new {@link Record} was created
	 */
	@PUT
	@Path("trainingtypes/{trty_pk}")
	@Consumes(MediaType.APPLICATION_JSON)
	public Boolean updateTrty(@PathParam("trty_pk") final Integer trty_pk, final Map<String, Object> trty) {
		return Transactions.with(this.ctx, transactionCtx -> {
			final boolean exists = this.ctx.fetchExists(TRAININGTYPES, TRAININGTYPES.TRTY_PK.eq(trty_pk));
			if (exists) {
				transactionCtx.update(TRAININGTYPES)
						.set(TRAININGTYPES.TRTY_NAME,           (String)  trty.get(TRAININGTYPES.TRTY_NAME.getName()))
						.set(TRAININGTYPES.TRTY_PRESENCEONLY,   (Boolean) trty.get(TRAININGTYPES.TRTY_PRESENCEONLY.getName()))
						.set(TRAININGTYPES.TRTY_EXTENDVALIDITY, (Boolean) trty.get(TRAININGTYPES.TRTY_EXTENDVALIDITY.getName()))
						.where(TRAININGTYPES.TRTY_PK.eq(trty_pk)).execute();

				// If "presenceOnly" training type, replace "FLUNKED" outcomes w/ "MISSING"
				if (Boolean.valueOf(String.valueOf(trty.get(TRAININGTYPES.TRTY_PRESENCEONLY.getName()))).booleanValue()) {
					transactionCtx.update(TRAININGS_EMPLOYEES)
							.set(TRAININGS_EMPLOYEES.TREM_OUTCOME, Constants.EMPL_OUTCOME_MISSING)
							.where(TRAININGS_EMPLOYEES.TREM_TRNG_FK.in(DSL
									.select(TRAININGS.TRNG_PK).from(TRAININGS).where(TRAININGS.TRNG_TRTY_FK.eq(trty_pk))))
							.and(TRAININGS_EMPLOYEES.TREM_OUTCOME.eq(Constants.EMPL_OUTCOME_FLUNKED))
							.execute();
				}
			} else {
				transactionCtx.insertInto(
											TRAININGTYPES,
											TRAININGTYPES.TRTY_PK,
											TRAININGTYPES.TRTY_NAME,
											TRAININGTYPES.TRTY_PRESENCEONLY,
											TRAININGTYPES.TRTY_EXTENDVALIDITY,
											TRAININGTYPES.TRTY_ORDER)
						.values(
								trty_pk,
								(String)  trty.get(TRAININGTYPES.TRTY_NAME.getName()),
								(Boolean) trty.get(TRAININGTYPES.TRTY_PRESENCEONLY.getName()),
								(Boolean) trty.get(TRAININGTYPES.TRTY_EXTENDVALIDITY.getName()),
								transactionCtx
										.select(DSL.coalesce(DSL.max(TRAININGTYPES.TRTY_ORDER), Integer.valueOf(0)).add(Integer.valueOf(1)).as("order"))
										.from(TRAININGTYPES)
										.fetchOne("order", Integer.class))
						.execute();

				transactionCtx
						.insertInto(TRAINERPROFILES_TRAININGTYPES, TRAINERPROFILES_TRAININGTYPES.TPTT_TRPR_FK, TRAINERPROFILES_TRAININGTYPES.TPTT_TRTY_FK)
						.values(Constants.DEFAULT_TRAINERPROFILE, trty_pk).execute();
			}

			transactionCtx.delete(TRAININGTYPES_CERTIFICATES).where(TRAININGTYPES_CERTIFICATES.TTCE_TRTY_FK.eq(trty_pk)).execute();
			@SuppressWarnings("unchecked")
			final Row2<Integer, Integer>[] certificates = ((Map<String, Integer>) trty.getOrDefault("certificates", Collections.EMPTY_MAP)).entrySet()
					.stream().map((entry) -> DSL.row(Integer.valueOf(entry.getKey()), entry.getValue())).toArray(Row2[]::new);

			if (certificates.length > 0) {
				transactionCtx.insertInto(
											TRAININGTYPES_CERTIFICATES,
											TRAININGTYPES_CERTIFICATES.TTCE_TRTY_FK,
											TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK,
											TRAININGTYPES_CERTIFICATES.TTCE_DURATION)
						.select(DSL.select(
											DSL.val(trty_pk),
											DSL.field("cert_pk", Integer.class),
											DSL.field("duration", Integer.class))
								.from(DSL.values(certificates).as("unused", "cert_pk", "duration")))
						.execute();
			}

			return Boolean.valueOf(!exists);
		});
	}

	@POST
	@SuppressWarnings({ "unchecked", "null" })
	@Path("reorder")
	public void reassignCertificates(final Map<Integer, Integer> reassignmentMap) {
		if (null == reassignmentMap) {
			return;
		}

		Transactions.with(this.ctx, transactionCtx -> {
			transactionCtx.update(CERTIFICATES)
					.set(CERTIFICATES.CERT_ORDER, DSL.field("new_order", Integer.class))
					.from(DSL.values(reassignmentMap.entrySet().stream().map(entry -> DSL.row(entry.getKey(), entry.getValue())).toArray(Row2[]::new))
							.as("unused", "pk", "new_order"))
					.where(CERTIFICATES.CERT_PK.eq(DSL.field("pk", Integer.class)))
					.execute();
			transactionCtx.execute(ResourcesHelper.cleanupSequence(CERTIFICATES, CERTIFICATES.CERT_PK, CERTIFICATES.CERT_ORDER));
		});
	}

	@POST
	@SuppressWarnings({ "unchecked", "null" })
	@Path("trainingtypes/reorder")
	public void reassignTrainingTypes(final Map<Integer, Integer> reassignmentMap) {
		if (null == reassignmentMap) {
			return;
		}

		Transactions.with(this.ctx, transactionCtx -> {
			transactionCtx.update(TRAININGTYPES)
					.set(TRAININGTYPES.TRTY_ORDER, DSL.field("new_order", Integer.class))
					.from(DSL.values(reassignmentMap.entrySet().stream().map(entry -> DSL.row(entry.getKey(), entry.getValue())).toArray(Row2[]::new))
							.as("unused", "pk", "new_order"))
					.where(TRAININGTYPES.TRTY_PK.eq(DSL.field("pk", Integer.class)))
					.execute();
			transactionCtx.execute(ResourcesHelper.cleanupSequence(TRAININGTYPES, TRAININGTYPES.TRTY_PK, TRAININGTYPES.TRTY_ORDER));
		});
	}

	@DELETE
	@Path("{cert_pk}")
	public void deleteCert(@PathParam("cert_pk") final Integer cert_pk) {
		Transactions.with(this.ctx, transactionCtx -> {
			transactionCtx.delete(CERTIFICATES).where(CERTIFICATES.CERT_PK.eq(cert_pk)).execute();
			transactionCtx.execute(ResourcesHelper.cleanupSequence(CERTIFICATES, CERTIFICATES.CERT_PK, CERTIFICATES.CERT_ORDER));
		});
	}

	@DELETE
	@Path("trainingtypes/{trty_pk}")
	public void deleteTrty(@PathParam("trty_pk") final Integer trty_pk) {
		Transactions.with(this.ctx, transactionCtx -> {
			transactionCtx.delete(TRAININGTYPES).where(TRAININGTYPES.TRTY_PK.eq(trty_pk)).execute();
			transactionCtx.execute(ResourcesHelper.cleanupSequence(TRAININGTYPES, TRAININGTYPES.TRTY_PK, TRAININGTYPES.TRTY_ORDER));
		});
	}
}
