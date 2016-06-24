package org.ccjmne.faomaintenance.api.utils;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;

import java.util.Map;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.ccjmne.faomaintenance.jooq.classes.tables.records.RolesRecord;
import org.jooq.DSLContext;
import org.jooq.Result;

@Singleton
@Path("account")
public class AccountEndpoint {

	private final DSLContext ctx;
	private final AdministrationEndpoint admin;

	@Inject
	public AccountEndpoint(final DSLContext ctx, final AdministrationEndpoint admin) {
		this.ctx = ctx;
		this.admin = admin;
	}

	@GET
	public Map<String, Object> getCurrentUserInfo(@Context final HttpServletRequest request) {
		return this.admin.getUserInfo(request.getRemoteUser());
	}

	@GET
	@Path("roles")
	public Result<RolesRecord> getAvailableRoles() {
		return this.admin.getAvailableRoles();
	}

	@PUT
	@Path("password")
	@Consumes(MediaType.APPLICATION_JSON)
	public void updatePassword(@Context final HttpServletRequest request, final Map<String, String> passwords) {
		final String currentPassword = passwords.get("pwd_current");
		final String newPassword = passwords.get("pwd_new");
		if ((currentPassword == null) || currentPassword.isEmpty() || (newPassword == null) || newPassword.isEmpty()) {
			throw new IllegalArgumentException("Both current and updated passwords must be provided.");
		}

		if (0 == this.ctx.update(EMPLOYEES).set(EMPLOYEES.EMPL_PWD, newPassword)
				.where(EMPLOYEES.EMPL_PK.eq(request.getRemoteUser()).and(EMPLOYEES.EMPL_PWD.eq(currentPassword))).execute()) {
			throw new IllegalArgumentException("Invalid current password.");
		}
	}
}
