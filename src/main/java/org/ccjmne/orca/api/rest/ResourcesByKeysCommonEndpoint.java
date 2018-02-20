package org.ccjmne.orca.api.rest;

import static org.ccjmne.orca.jooq.classes.Tables.CERTIFICATES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGTYPES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGTYPES_CERTIFICATES;

import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import org.ccjmne.orca.api.modules.ResourcesUnrestricted;
import org.ccjmne.orca.jooq.classes.tables.records.CertificatesRecord;
import org.ccjmne.orca.jooq.classes.tables.records.TrainingtypesCertificatesRecord;
import org.ccjmne.orca.jooq.classes.tables.records.TrainingtypesRecord;
import org.jooq.Result;

/**
 * Serves the resources whose access isn't restricted.<br />
 * Unlike {@link ResourcesCommonEndpoint}, this API presents resources into
 * {@link Map}s keyed by their unique identifier.
 *
 * @author ccjmne
 */
@Path("resources-by-keys-common")
// TODO: resources-common-by-keys?
// TODO: merge with ResourcesCommonEndpoint?
public class ResourcesByKeysCommonEndpoint {

	private final ResourcesUnrestricted resources;

	@Inject
	public ResourcesByKeysCommonEndpoint(final ResourcesUnrestricted resources) {
		this.resources = resources;
	}

	@GET
	@Path("trainingtypes")
	public Map<Integer, TrainingtypesRecord> listTrainingTypes() {
		return this.resources.listTrainingTypes().intoMap(TRAININGTYPES.TRTY_PK);
	}

	@GET
	@Path("trainingtypes_certificates")
	public Map<Integer, Result<TrainingtypesCertificatesRecord>> listTrainingtypesCertificates() {
		return this.resources.listTrainingTypesCertificates().intoGroups(TRAININGTYPES_CERTIFICATES.TTCE_TRTY_FK);
	}

	@GET
	@Path("certificates")
	public Map<Integer, CertificatesRecord> listCertificates() {
		return this.resources.listCertificates().intoMap(CERTIFICATES.CERT_PK);
	}
}
