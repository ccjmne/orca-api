package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;

import java.util.Base64;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.ccjmne.faomaintenance.api.utils.AdministrationEndpoint;
import org.jooq.DSLContext;

@Singleton
@Path("auth")
public class AuthenticationEndpoint {

	private final AdministrationEndpoint admin;
	private final DSLContext ctx;

	@Inject
	public AuthenticationEndpoint(final DSLContext ctx, final AdministrationEndpoint admin) {
		this.ctx = ctx;
		this.admin = admin;
	}

	@POST
	public Response authenticate(final String authorization) {
		final String[] split = new String(Base64.getDecoder().decode(authorization)).split(":");
		if ((split.length == 2) && this.ctx.fetchExists(EMPLOYEES, EMPLOYEES.EMPL_PK.eq(split[0]).and(EMPLOYEES.EMPL_PWD.eq(split[1])))) {
			return Response.ok(this.admin.getUserInfo(split[0])).build();
		}

		return Response.status(Status.UNAUTHORIZED).build();
	}
}
