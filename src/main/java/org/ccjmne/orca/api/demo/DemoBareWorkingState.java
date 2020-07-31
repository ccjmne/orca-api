package org.ccjmne.orca.api.demo;

import static org.ccjmne.orca.jooq.codegen.Tables.CLIENT;
import static org.ccjmne.orca.jooq.codegen.Tables.EMPLOYEES;
import static org.ccjmne.orca.jooq.codegen.Tables.SITES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAINERPROFILES;
import static org.ccjmne.orca.jooq.codegen.Tables.USERS;
import static org.ccjmne.orca.jooq.codegen.Tables.USERS_ROLES;

import java.time.LocalDate;

import org.ccjmne.orca.api.rest.pub.ClientEndpoint;
import org.ccjmne.orca.api.utils.Constants;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Sequence;
import org.jooq.TableField;
import org.jooq.impl.DSL;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectRequest;

public class DemoBareWorkingState {

  public static final String DEMO_PASSWORD = "pwd";

  private static final String DEMO_CLIENT_ID     = "demo";
  private static final String DEMO_CLIENT_NAME   = "Version de démonstration";
  private static final String DEMO_CLIENT_MAILTO = "infos@orca-solution.com";
  private static final String DEMO_CLIENT_LOGO   = "https://s3-eu-west-1.amazonaws.com/orca-resources/logo_complete.png";

  private static final String DEMO_TRAINERPROFILE = "Tous types de formation";

  public static void restore(final DSLContext ctx, final AmazonS3Client client) {
    // Clear all data
    ctx.meta().getTables().forEach(table -> ctx.truncate(table).restartIdentity().cascade().execute());
    // Reset all sequences. It *has* to go through a concrete class, unlike
    // truncating Tables, which can be done through ctx.meta()
    CLIENT.getSchema().getSequences().forEach(sequence -> ctx.alterSequence(sequence).restart().execute());

    // Delete S3 resources
    final String objectKey = String.format("%s-welcome.json", ctx.selectFrom(CLIENT).fetchOne(CLIENT.CLNT_ID));
    if (client.doesObjectExist(ClientEndpoint.ORCA_RESOURCES_BUCKET, objectKey)) {
      client.deleteObject(new DeleteObjectRequest(ClientEndpoint.ORCA_RESOURCES_BUCKET, objectKey));
    }

    ctx.insertInto(CLIENT, CLIENT.CLNT_ID, CLIENT.CLNT_NAME, CLIENT.CLNT_MAILTO, CLIENT.CLNT_LOGO, CLIENT.CLNT_LIVECHAT)
        .values(
                DEMO_CLIENT_ID,
                DEMO_CLIENT_NAME,
                DEMO_CLIENT_MAILTO,
                DEMO_CLIENT_LOGO,
                Boolean.TRUE)
        .execute();

    ctx.insertInto(SITES, SITES.SITE_PK, SITES.SITE_NAME, SITES.SITE_EXTERNAL_ID)
        .values(Constants.DECOMMISSIONED_SITE, "", "::decommissioned::site")
        .execute();
    ctx.alterSequence(DemoBareWorkingState.sequenceFor(SITES.SITE_PK)).restartWith(Integer.valueOf(1)).execute();

    ctx.insertInto(
                   EMPLOYEES,
                   EMPLOYEES.EMPL_PK,
                   EMPLOYEES.EMPL_FIRSTNAME,
                   EMPLOYEES.EMPL_SURNAME,
                   EMPLOYEES.EMPL_DOB,
                   EMPLOYEES.EMPL_PERMANENT,
                   EMPLOYEES.EMPL_GENDER,
                   EMPLOYEES.EMPL_EXTERNAL_ID)
        .values(
                Constants.EMPLOYEE_ROOT,
                "Admin",
                DEMO_CLIENT_ID,
                LocalDate.now(),
                Boolean.valueOf(false),
                Boolean.valueOf(false),
                "::root::employee")
        .execute();
    ctx.alterSequence(DemoBareWorkingState.sequenceFor(EMPLOYEES.EMPL_PK)).restartWith(Integer.valueOf(1)).execute();

    ctx.insertInto(USERS, USERS.USER_ID, USERS.USER_PWD, USERS.USER_TYPE, USERS.USER_EMPL_FK)
        .values(DSL.val(Constants.USER_ROOT), DSL.md5(DEMO_PASSWORD), DSL.val(Constants.USERTYPE_EMPLOYEE), DSL.val(Constants.EMPLOYEE_ROOT))
        .execute();

    ctx.insertInto(TRAINERPROFILES, TRAINERPROFILES.TRPR_PK, TRAINERPROFILES.TRPR_ID)
        .values(Constants.DEFAULT_TRAINERPROFILE, DEMO_TRAINERPROFILE)
        .execute();

    // All roles except account management
    ctx.insertInto(USERS_ROLES, USERS_ROLES.USER_ID, USERS_ROLES.USRO_TYPE, USERS_ROLES.USRO_LEVEL, USERS_ROLES.USRO_TRPR_FK)
        .values(Constants.USER_ROOT, Constants.ROLE_ACCESS, Integer.valueOf(4), null)
        .values(Constants.USER_ROOT, Constants.ROLE_TRAINER, null, Constants.DEFAULT_TRAINERPROFILE)
        .values(Constants.USER_ROOT, Constants.ROLE_ADMIN, Integer.valueOf(4), null)
        .execute();
  }

  @SuppressWarnings("unchecked")
  public static <T extends Number> Sequence<T> sequenceFor(final TableField<? extends Record, T> ownedBy) {
    return (Sequence<T>) CLIENT.getSchema().getSequence(String.format("%s_%s_seq", ownedBy.getTable().getName(), ownedBy.getName()));
  }
}
