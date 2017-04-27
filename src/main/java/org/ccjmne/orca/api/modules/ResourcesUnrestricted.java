package org.ccjmne.orca.api.modules;

import static org.ccjmne.orca.jooq.classes.Tables.CERTIFICATES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGTYPES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGTYPES_CERTIFICATES;

import javax.inject.Inject;

import org.ccjmne.orca.jooq.classes.tables.records.CertificatesRecord;
import org.ccjmne.orca.jooq.classes.tables.records.TrainingtypesCertificatesRecord;
import org.ccjmne.orca.jooq.classes.tables.records.TrainingtypesRecord;
import org.jooq.DSLContext;
import org.jooq.Result;

/**
 * Concentrate all accesses to the database (all usages of a {@link DSLContext})
 * that are not meant to be filtered according to the {@link Restrictions}
 * module.
 */
public class ResourcesUnrestricted {

	private final DSLContext ctx;

	@Inject
	public ResourcesUnrestricted(final DSLContext ctx) {
		this.ctx = ctx;
	}

	public Result<TrainingtypesRecord> listTrainingTypes() {
		return this.ctx.selectFrom(TRAININGTYPES).orderBy(TRAININGTYPES.TRTY_ORDER).fetch();
	}

	public Result<TrainingtypesCertificatesRecord> listTrainingTypesCertificates() {
		return this.ctx.selectFrom(TRAININGTYPES_CERTIFICATES).fetch();
	}

	public Result<CertificatesRecord> listCertificates() {
		return this.ctx.selectFrom(CERTIFICATES).orderBy(CERTIFICATES.CERT_ORDER).fetch();
	}
}
