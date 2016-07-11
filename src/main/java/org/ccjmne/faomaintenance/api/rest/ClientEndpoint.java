package org.ccjmne.faomaintenance.api.rest;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import org.ccjmne.faomaintenance.jooq.classes.Tables;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.ClientRecord;
import org.jooq.DSLContext;

@Path("client")
public class ClientEndpoint {

	private final DSLContext ctx;

	@Inject
	public ClientEndpoint(final DSLContext ctx) {
		this.ctx = ctx;
	}

	@GET
	public ClientRecord getClientInfo() {
		return this.ctx.fetchOne(Tables.CLIENT);
	}
}
