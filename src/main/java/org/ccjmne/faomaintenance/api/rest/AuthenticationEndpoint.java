package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.USERS;

import java.util.Base64;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;

@Path("auth")
public class AuthenticationEndpoint {

	private final DSLContext ctx;

	@Inject
	public AuthenticationEndpoint(final DSLContext ctx) {
		this.ctx = ctx;
	}

	@POST
	public Response authenticate(final String authorization) {
		final String[] split = new String(Base64.getDecoder().decode(authorization)).split(":");
		if ((split.length == 2) && this.ctx.fetchExists(USERS, USERS.USER_ID.eq(split[0]).and(USERS.USER_PWD.eq(DSL.md5(split[1]))))) {
			return Response.ok(UsersEndpoint.getUserInfoImpl(split[0], this.ctx)).build();
		}

		return Response.status(Status.UNAUTHORIZED).build();
	}
}
