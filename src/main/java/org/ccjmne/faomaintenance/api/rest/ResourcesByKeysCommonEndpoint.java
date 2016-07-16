package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.CERTIFICATES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGTYPES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGTYPES_CERTIFICATES;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import org.ccjmne.faomaintenance.api.modules.ResourcesUnrestricted;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.CertificatesRecord;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.TrainingtypesRecord;

@Path("resources-by-keys-common")
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
	public Map<Integer, List<Integer>> listTrainingtypesCertificates() {
		return this.resources.listTrainingTypesCertificates().intoGroups(TRAININGTYPES_CERTIFICATES.TTCE_TRTY_FK, TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK);
	}

	@GET
	@Path("certificates")
	public Map<Integer, CertificatesRecord> listCertificates() {
		return this.resources.listCertificates().intoMap(CERTIFICATES.CERT_PK);
	}
}
