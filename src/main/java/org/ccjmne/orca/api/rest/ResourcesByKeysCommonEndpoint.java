package org.ccjmne.orca.api.rest;

import static org.ccjmne.orca.jooq.classes.Tables.CERTIFICATES;
import static org.ccjmne.orca.jooq.classes.Tables.TAGS;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGTYPES;

import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import org.ccjmne.orca.api.modules.ResourcesUnrestricted;
import org.ccjmne.orca.jooq.classes.tables.records.CertificatesRecord;

import com.google.common.collect.Maps;

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
	public Map<Integer, Map<String, Object>> listTrainingTypes() {
		return Maps.uniqueIndex(this.resources.listTrainingTypes(), trty -> (Integer) trty.get(TRAININGTYPES.TRTY_PK.getName()));
	}

	@GET
	@Path("certificates")
	public Map<Integer, CertificatesRecord> listCertificates() {
		return this.resources.listCertificates().intoMap(CERTIFICATES.CERT_PK);
	}

	@GET
	@Path("tags")
	public Map<Integer, Map<String, Object>> listTags(@QueryParam("type") final Integer type) {
		return Maps.uniqueIndex(this.resources.listTags(type), x -> (Integer) x.get(TAGS.TAGS_PK.getName()));
	}
}
