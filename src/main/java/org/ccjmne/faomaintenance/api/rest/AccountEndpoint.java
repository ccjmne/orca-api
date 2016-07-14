package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAINERPROFILES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAINERPROFILES_TRAININGTYPES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.USERS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.USERS_ROLES;

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
	@Path("trainerprofile")
	public Record getTrainerProfiles(@Context final HttpServletRequest request) {
		return this.ctx.select(TRAINERPROFILES.TRPR_PK, TRAINERPROFILES.TRPR_ID, DSL.arrayAgg(TRAINERPROFILES_TRAININGTYPES.TPTT_TRTY_FK).as("types"))
				.from(TRAINERPROFILES).leftOuterJoin(TRAINERPROFILES_TRAININGTYPES).on(TRAINERPROFILES_TRAININGTYPES.TPTT_TRPR_FK.eq(TRAINERPROFILES.TRPR_PK))
				.where(TRAINERPROFILES.TRPR_PK
						.eq(DSL.select(USERS_ROLES.USRO_TRPR_FK).from(USERS_ROLES)
								.where(USERS_ROLES.USER_ID.eq(request.getRemoteUser()).and(USERS_ROLES.USRO_TYPE.eq(Constants.ROLE_TRAINER)))
								.asField()))
				.groupBy(TRAINERPROFILES.TRPR_PK, TRAINERPROFILES.TRPR_ID).fetchOne();
	}

	@PUT
	@Path("password")
	@Consumes(MediaType.APPLICATION_JSON)
	// TODO: use Restrictions
	public void updatePassword(@Context final HttpServletRequest request, final Map<String, String> passwords) {
		final String currentPassword = passwords.get("pwd_current");
		final String newPassword = passwords.get("pwd_new");
		if ((currentPassword == null) || currentPassword.isEmpty() || (newPassword == null) || newPassword.isEmpty()) {
			throw new IllegalArgumentException("Both current and updated passwords must be provided.");
		}

		if (0 == this.ctx.update(USERS).set(USERS.USER_PWD, DSL.md5(newPassword))
				.where(USERS.USER_ID.eq(request.getRemoteUser()).and(USERS.USER_PWD.eq(DSL.md5(currentPassword)))).execute()) {
			throw new IllegalArgumentException("Either the user doesn't exist or the specified current password was incorrect.");
		}
	}
}
