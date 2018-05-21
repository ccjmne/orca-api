package org.ccjmne.orca.api.rest;

import static org.ccjmne.orca.jooq.classes.Tables.TAGS;

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
import org.ccjmne.orca.api.utils.ResourcesHelper;
import org.ccjmne.orca.api.utils.Transactions;
import org.ccjmne.orca.jooq.classes.Sequences;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.impl.DSL;

@Path("tags")
public class TagsEndpoint {

	private final DSLContext ctx;

	@Inject
	public TagsEndpoint(final Restrictions restrictions, final DSLContext ctx) {
		if (!restrictions.canManageSitesAndTags()) {
			throw new ForbiddenException();
		}

		this.ctx = ctx;
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	public Integer createTag(final Map<String, String> tagDefinition) {
		final Integer tags_pk = new Integer(this.ctx.nextval(Sequences.TAGS_TAGS_PK_SEQ).intValue());
		this.updateTag(tags_pk, tagDefinition);
		return tags_pk;
	}

	/**
	 * @return <code>true</code> iff a new {@link Record} was created
	 */
	@PUT
	@Path("{tags_pk}")
	@Consumes(MediaType.APPLICATION_JSON)
	public Boolean updateTag(@PathParam("tags_pk") final Integer tags_pk, final Map<String, String> tagDefinition) {
		return Transactions.with(this.ctx, transactionCtx -> {
			final boolean exists = transactionCtx.fetchExists(TAGS, TAGS.TAGS_PK.eq(tags_pk));
			if (exists) {
				transactionCtx.update(TAGS).set(tagDefinition).where(TAGS.TAGS_PK.eq(tags_pk)).execute();
			} else {
				transactionCtx.insertInto(TAGS)
						.set(tagDefinition)
						.set(TAGS.TAGS_ORDER, DSL.select(DSL.coalesce(DSL.max(TAGS.TAGS_ORDER), Integer.valueOf(0)).add(Integer.valueOf(1))).from(TAGS))
						.execute();
			}

			return Boolean.valueOf(!exists);
		});
	}

	@DELETE
	@Path("{tags_pk}")
	public void deleteTag(@PathParam("tags_pk") final Integer tags_pk) {
		Transactions.with(this.ctx, transactionCtx -> {
			transactionCtx.delete(TAGS).where(TAGS.TAGS_PK.eq(tags_pk)).execute();
			transactionCtx.execute(ResourcesHelper.cleanupSequence(TAGS, TAGS.TAGS_PK, TAGS.TAGS_ORDER));
		});
	}
}
