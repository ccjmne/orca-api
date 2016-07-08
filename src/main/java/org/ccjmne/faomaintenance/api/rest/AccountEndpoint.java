package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES_ROLES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAINERPROFILES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAINERPROFILES_TRAININGTYPES;

import java.util.Map;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.ccjmne.faomaintenance.api.utils.Constants;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.impl.DSL;

@Path("account")
public class AccountEndpoint {

	private final DSLContext ctx;

	@Inject
	public AccountEndpoint(final DSLContext ctx) {
		this.ctx = ctx;
	}

	@GET
	public Map<String, Object> getCurrentUserInfo(@Context final HttpServletRequest request) {
		return AdministrationEndpoint.getUserInfoImpl(request.getRemoteUser(), this.ctx);
	}

	@GET
	@Path("trainerlevel")
	public Record getTrainerLevels(@Context final HttpServletRequest request) {
		return this.ctx.select(TRAINERPROFILES.TRPR_PK, TRAINERPROFILES.TRPR_ID, DSL.arrayAgg(TRAINERPROFILES_TRAININGTYPES.TPTT_TRTY_FK).as("types"))
				.from(TRAINERPROFILES).leftOuterJoin(TRAINERPROFILES_TRAININGTYPES).on(TRAINERPROFILES_TRAININGTYPES.TPTT_TRPR_FK.eq(TRAINERPROFILES.TRPR_PK))
				.where(TRAINERPROFILES.TRPR_PK
						.eq(DSL.select(EMPLOYEES_ROLES.EMRO_TRPR_FK).from(EMPLOYEES_ROLES)
								.where(EMPLOYEES_ROLES.EMPL_PK.eq(request.getRemoteUser()).and(EMPLOYEES_ROLES.EMRO_TYPE.eq(Constants.ROLE_TRAINER)))
								.asField()))
				.groupBy(TRAINERPROFILES.TRPR_PK, TRAINERPROFILES.TRPR_ID).fetchOne();
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

		if (0 == this.ctx.update(EMPLOYEES).set(EMPLOYEES.EMPL_PWD, DSL.md5(newPassword))
				.where(EMPLOYEES.EMPL_PK.eq(request.getRemoteUser()).and(EMPLOYEES.EMPL_PWD.eq(currentPassword))).execute()) {
			throw new IllegalArgumentException("Invalid current password.");
		}
	}
}
