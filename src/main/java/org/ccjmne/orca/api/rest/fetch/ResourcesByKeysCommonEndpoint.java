package org.ccjmne.orca.api.rest.fetch;

import static org.ccjmne.orca.jooq.codegen.Tables.CERTIFICATES;
import static org.ccjmne.orca.jooq.codegen.Tables.TAGS;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGTYPES;

import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import org.ccjmne.orca.api.modules.ResourcesUnrestricted;
import org.ccjmne.orca.jooq.codegen.tables.records.CertificatesRecord;

import com.google.common.collect.Maps;

/**
 * Serves the resources whose access isn't restricted.<br />
 * Unlike {@link ResourcesCommonEndpoint}, this API presents resources into
 * {@link Map}s keyed by their unique identifier.
 *
 * @author ccjmne
 */
@Path("resources-common/keyed")
// TODO: merge with ResourcesCommonEndpoint?
public class ResourcesByKeysCommonEndpoint {

	private final ResourcesUnrestricted resources;

	@Inject
	public ResourcesByKeysCommonEndpoint(final ResourcesUnrestricted resources) {
		this.resources = resources;
	}

	@GET
	@Path("trainingtypes")
	public Map<Integer, Map<String, Object>> listTrainingTypes(@QueryParam("date") final String dateStr) {
		return Maps.uniqueIndex(this.resources.listTrainingTypes(dateStr), trty -> (Integer) trty.get(TRAININGTYPES.TRTY_PK.getName()));
	}

	@GET
	@Path("certificates")
	public Map<Integer, CertificatesRecord> listCertificates() {
		return this.resources.listCertificates().intoMap(CERTIFICATES.CERT_PK);
	}

	@GET
	@Path("tags")
	public Map<Integer, Map<String, Object>> listTags() {
		return Maps.uniqueIndex(this.resources.listTags(), x -> (Integer) x.get(TAGS.TAGS_PK.getName()));
	}
}
