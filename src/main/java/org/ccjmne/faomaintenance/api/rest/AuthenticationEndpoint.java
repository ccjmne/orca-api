package org.ccjmne.faomaintenance.api.rest;

import java.util.Map;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;

@Singleton
@Path("auth")
public class AuthenticationEndpoint {

	private final AdministrationEndpoint admin;

	@Inject
	public AuthenticationEndpoint(final AdministrationEndpoint admin) {
		this.admin = admin;
	}

	@GET
	public Map<String, Object> authenticate(@Context final HttpServletRequest request) {
		return this.admin.getUserInfo(request.getRemoteUser());
	}
}
