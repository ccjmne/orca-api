package org.ccjmne.orca.api.rest.fetch;

import static org.ccjmne.orca.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES_TAGS;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS;

import java.text.ParseException;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import org.ccjmne.orca.api.modules.Restrictions;
import org.jooq.Record;

import com.google.common.collect.Maps;

/**
 * Serves the resources whose access is restricted based on the request's
 * associated {@link Restrictions}.<br />
 * Unlike {@link ResourcesEndpoint}, this API presents resources into
 * {@link Map}s keyed by their unique identifier.
 *
 * @author ccjmne
 */
@Path("resources/keyed")
// TODO: merge with ResourcesEndpoint?
public class ResourcesByKeysEndpoint {

	final ResourcesEndpoint resources;

	@Inject
	public ResourcesByKeysEndpoint(final ResourcesEndpoint resources) {
		this.resources = resources;
	}

	@GET
	@Path("employees")
	public Map<Integer, Map<String, Object>> listEmployees(
															@QueryParam("employee") final Integer empl_pk,
															@QueryParam("site") final Integer site_pk,
															@QueryParam("training") final Integer trng_pk,
															@QueryParam("date") final String dateStr,
															@QueryParam("fields") final String fields,
															@Context final UriInfo uriInfo) {
		return Maps.uniqueIndex(this.resources.listEmployees(empl_pk, site_pk, trng_pk, dateStr, fields, uriInfo),
								entry -> (Integer) entry.get(EMPLOYEES.EMPL_PK.getName()));
	}

	@GET
	@Path("sites")
	public Map<Integer, Map<String, Object>> listSites(
														@QueryParam("site") final Integer site_pk,
														@QueryParam("date") final String dateStr,
														@QueryParam("unlisted") final boolean unlisted,
														@Context final UriInfo uriInfo) {
		return Maps.uniqueIndex(this.resources.listSites(site_pk, dateStr, unlisted, uriInfo),
								entry -> (Integer) entry.get(SITES.SITE_PK.getName()));
	}

	@GET
	@Path("sites-groups")
	public Map<String, Map<String, Object>> listSitesGroups(
															@QueryParam("group-by") final Integer tags_pk,
															@QueryParam("date") final String dateStr,
															@QueryParam("unlisted") final boolean unlisted,
															@Context final UriInfo uriInfo) {
		return Maps.uniqueIndex(this.resources.listSitesGroups(tags_pk, dateStr, unlisted, uriInfo),
								entry -> String.valueOf(entry.get(SITES_TAGS.SITA_VALUE.getName())));
	}

	/**
	 * Delegates to
	 * {@link ResourcesByKeysEndpoint#listSitesGroups(String, boolean, Integer, UriInfo)}.<br
	 * />
	 * With this method, the <code>tags_pk</code> argument comes directly from
	 * the query's <strong>path</strong> instead of its parameters.
	 *
	 * @param tags_pk
	 *            The tag to group sites by.
	 */
	@GET
	@Path("sites-groups/{group-by}")
	public Map<String, Map<String, Object>> listSitesGroupsBy(
																@PathParam("group-by") final Integer tags_pk,
																@QueryParam("date") final String dateStr,
																@QueryParam("unlisted") final boolean unlisted,
																@Context final UriInfo uriInfo) {
		return this.listSitesGroups(tags_pk, dateStr, unlisted, uriInfo);
	}

	@GET
	@Path("trainings")
	public Map<Integer, Record> listTrainings(
												@QueryParam("employee") final Integer empl_pk,
												@QueryParam("type") final List<Integer> types,
												@QueryParam("date") final String dateStr,
												@QueryParam("from") final String fromStr,
												@QueryParam("to") final String toStr,
												@QueryParam("completed") final Boolean completedOnly)
			throws ParseException {
		return this.resources.listTrainings(empl_pk, types, dateStr, fromStr, toStr, completedOnly).intoMap(TRAININGS.TRNG_PK);
	}
}
