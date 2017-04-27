package org.ccjmne.orca.api.rest;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import org.ccjmne.faomaintenance.jooq.classes.tables.records.CertificatesRecord;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.TrainingtypesCertificatesRecord;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.TrainingtypesRecord;
import org.ccjmne.orca.api.modules.ResourcesUnrestricted;
import org.jooq.Result;

@Path("resources-common")
public class ResourcesCommonEndpoint {

	private final ResourcesUnrestricted delegate;

	@Inject
	public ResourcesCommonEndpoint(final ResourcesUnrestricted delegate) {
		this.delegate = delegate;
	}

	@GET
	@Path("trainingtypes")
	public Result<TrainingtypesRecord> listTrainingTypes() {
		return this.delegate.listTrainingTypes();
	}

	@GET
	@Path("trainingtypes_certificates")
	public Result<TrainingtypesCertificatesRecord> listTrainingTypesCertificates() {
		return this.delegate.listTrainingTypesCertificates();
	}

	@GET
	@Path("certificates")
	public Result<CertificatesRecord> listCertificates() {
		return this.delegate.listCertificates();
	}
}
