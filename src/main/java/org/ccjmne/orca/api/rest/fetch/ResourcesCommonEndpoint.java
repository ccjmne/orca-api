package org.ccjmne.orca.api.rest.fetch;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import org.ccjmne.orca.api.modules.ResourcesUnrestricted;
import org.ccjmne.orca.jooq.codegen.tables.records.CertificatesRecord;
import org.jooq.Result;

/**
 * Serves the resources whose access isn't restricted.
 *
 * @author ccjmne
 */
@Path("resources-common")
public class ResourcesCommonEndpoint {

	private final ResourcesUnrestricted delegate;

	@Inject
	public ResourcesCommonEndpoint(final ResourcesUnrestricted delegate) {
		this.delegate = delegate;
	}

	@GET
	@Path("trainingtypes")
	public List<Map<String, Object>> listTrainingTypes(@QueryParam("date") final String dateStr) {
		return this.delegate.listTrainingTypes(dateStr);
	}

	@GET
	@Path("certificates")
	public Result<CertificatesRecord> listCertificates() {
		return this.delegate.listCertificates();
	}

	@GET
	@Path("tags")
	public List<Map<String, Object>> listTags() {
		return this.delegate.listTags();
	}
}
