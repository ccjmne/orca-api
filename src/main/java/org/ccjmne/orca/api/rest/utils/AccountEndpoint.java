package org.ccjmne.orca.api.rest.utils;

import static org.ccjmne.orca.jooq.codegen.Tables.TRAINERPROFILES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAINERPROFILES_TRAININGTYPES;
import static org.ccjmne.orca.jooq.codegen.Tables.USERS;
import static org.ccjmne.orca.jooq.codegen.Tables.USERS_CERTIFICATES;
import static org.ccjmne.orca.jooq.codegen.Tables.USERS_ROLES;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;

import org.ccjmne.orca.api.inject.business.Restrictions;
import org.ccjmne.orca.api.rest.admin.UsersEndpoint;
import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.api.utils.Transactions;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Row1;
import org.jooq.impl.DSL;

@Path("account")
public class AccountEndpoint {

  private final String       userId;
  private final DSLContext   ctx;
  private final Restrictions restrictions;

  @Inject
  public AccountEndpoint(final DSLContext ctx, final Restrictions restrictions) {
    this.restrictions = restrictions;
    this.ctx = ctx;
    this.userId = restrictions.getUserId();
  }

  @GET
  public Map<String, Object> getCurrentUserInfo() {
    return UsersEndpoint.getUserInfoImpl(this.userId, this.ctx);
  }

  @GET
  @Path("trainerprofile")
  public Record getTrainerProfiles() {
    return this.ctx.select(TRAINERPROFILES.TRPR_PK, TRAINERPROFILES.TRPR_ID, DSL.arrayAgg(TRAINERPROFILES_TRAININGTYPES.TPTT_TRTY_FK).as("types"))
        .from(TRAINERPROFILES).leftOuterJoin(TRAINERPROFILES_TRAININGTYPES).on(TRAINERPROFILES_TRAININGTYPES.TPTT_TRPR_FK.eq(TRAINERPROFILES.TRPR_PK))
        .where(TRAINERPROFILES.TRPR_PK
            .eq(DSL.select(USERS_ROLES.USRO_TRPR_FK).from(USERS_ROLES)
                .where(USERS_ROLES.USER_ID.eq(this.userId).and(USERS_ROLES.USRO_TYPE.eq(Constants.ROLE_TRAINER)))
                .asField()))
        .groupBy(TRAINERPROFILES.TRPR_PK, TRAINERPROFILES.TRPR_ID).fetchOne();
  }

  @PUT
  @Path("password")
  @Consumes(MediaType.APPLICATION_JSON)
  public void updatePassword(final Map<String, String> passwords) {
    if (!this.restrictions.canManageOwnAccount()) {
      throw new ForbiddenException("This account cannot update its own password");
    }

    final String currentPassword = passwords.get("pwd_current");
    final String newPassword = passwords.get("pwd_new");
    if ((currentPassword == null) || currentPassword.isEmpty() || (newPassword == null) || newPassword.isEmpty()) {
      throw new IllegalArgumentException("Both current and updated passwords must be provided.");
    }

    if (0 == this.ctx.update(USERS).set(USERS.USER_PWD, DSL.md5(newPassword))
        .where(USERS.USER_ID.eq(this.userId).and(USERS.USER_PWD.eq(DSL.md5(currentPassword)))).execute()) {
      throw new IllegalArgumentException("Either the user doesn't exist or the specified current password was incorrect.");
    }
  }

  @PUT
  @Path("id/{new_id}")
  public void changeId(@PathParam("new_id") final String newId) {
    if (!this.restrictions.canManageOwnAccount()) {
      throw new ForbiddenException("This account cannot change its own ID");
    }

    UsersEndpoint.changeIdImpl(this.userId, newId, this.ctx);
  }

  @GET
  @Path("observed-certificates")
  public List<Integer> getRelevantCertificates() {
    return this.ctx.selectFrom(USERS_CERTIFICATES)
        .where(USERS_CERTIFICATES.USCE_USER_FK.eq(this.userId))
        .fetch(USERS_CERTIFICATES.USCE_CERT_FK);
  }

  @PUT
  @Path("observed-certificates")
  @SuppressWarnings("unchecked")
  public void setRelevantCertificates(final List<Integer> certificates) {
    Transactions.with(this.ctx, transactionCtx -> {
      transactionCtx.delete(USERS_CERTIFICATES).where(USERS_CERTIFICATES.USCE_USER_FK.eq(this.userId)).execute();
      if (!certificates.isEmpty()) {
        transactionCtx.insertInto(USERS_CERTIFICATES, USERS_CERTIFICATES.USCE_USER_FK, USERS_CERTIFICATES.USCE_CERT_FK)
            .select(DSL.select(DSL.val(this.userId), DSL.field("cert_id", Integer.class))
                .from(DSL.values(certificates.stream().map(DSL::row).toArray(Row1[]::new)).as("unused", "cert_id")))
            .execute();
      }
    });
  }
}
