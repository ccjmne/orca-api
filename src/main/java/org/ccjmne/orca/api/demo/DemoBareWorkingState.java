package org.ccjmne.orca.api.demo;

import static org.ccjmne.orca.jooq.classes.Tables.CLIENT;
import static org.ccjmne.orca.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAINERPROFILES;
import static org.ccjmne.orca.jooq.classes.Tables.USERS;
import static org.ccjmne.orca.jooq.classes.Tables.USERS_ROLES;

import java.util.Date;

import org.ccjmne.orca.api.rest.ClientEndpoint;
import org.ccjmne.orca.api.utils.Constants;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectRequest;

public class DemoBareWorkingState {

	private static final String DEMO_PASSWORD = "pwd";

	private static final String DEMO_CLIENT_ID = "demo";
	private static final String DEMO_CLIENT_NAME = "Version de démonstration";
	private static final String DEMO_CLIENT_MAILTO = "nclsdevelopment@gmail.com";
	private static final String DEMO_CLIENT_LOGO = "https://s3-eu-west-1.amazonaws.com/orca-resources/logo_complete.png";

	private static final String DEMO_TRAINERPROFILE = "Tous types de formation";

	public static void restore(final DSLContext ctx, final AmazonS3Client client) {
		// Clear all data
		ctx.meta().getTables().forEach(table -> ctx.truncate(table).cascade().execute());
		// Reset all sequences
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

		ctx.insertInto(SITES, SITES.SITE_PK, SITES.SITE_NAME)
				.values(Constants.UNASSIGNED_SITE, "")
				.execute();

		ctx.insertInto(
						EMPLOYEES,
						EMPLOYEES.EMPL_PK,
						EMPLOYEES.EMPL_FIRSTNAME,
						EMPLOYEES.EMPL_SURNAME,
						EMPLOYEES.EMPL_DOB,
						EMPLOYEES.EMPL_PERMANENT,
						EMPLOYEES.EMPL_GENDER)
				.values(
						Constants.USER_ROOT,
						"Admin",
						DEMO_CLIENT_ID,
						new java.sql.Date(new Date().getTime()),
						Boolean.valueOf(false),
						Boolean.valueOf(false))
				.execute();

		ctx.insertInto(USERS, USERS.USER_ID, USERS.USER_PWD, USERS.USER_TYPE, USERS.USER_EMPL_FK)
				.values(DSL.val(Constants.USER_ROOT), DSL.md5(DEMO_PASSWORD), DSL.val(Constants.USERTYPE_EMPLOYEE), DSL.val(Constants.USER_ROOT))
				.execute();

		ctx.insertInto(TRAINERPROFILES, TRAINERPROFILES.TRPR_PK, TRAINERPROFILES.TRPR_ID)
				.values(Constants.UNASSIGNED_TRAINERPROFILE, DEMO_TRAINERPROFILE)
				.execute();

		// All roles except account management
		ctx.insertInto(USERS_ROLES, USERS_ROLES.USER_ID, USERS_ROLES.USRO_TYPE, USERS_ROLES.USRO_LEVEL, USERS_ROLES.USRO_TRPR_FK)
				.values(Constants.USER_ROOT, Constants.ROLE_ACCESS, Integer.valueOf(4), null)
				.values(Constants.USER_ROOT, Constants.ROLE_TRAINER, null, Constants.UNASSIGNED_TRAINERPROFILE)
				.values(Constants.USER_ROOT, Constants.ROLE_ADMIN, Integer.valueOf(4), null)
				.execute();

	}
}
