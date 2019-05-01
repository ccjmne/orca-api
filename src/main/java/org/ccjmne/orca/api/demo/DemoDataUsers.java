package org.ccjmne.orca.api.demo;

import static org.ccjmne.orca.jooq.codegen.Tables.EMPLOYEES;
import static org.ccjmne.orca.jooq.codegen.Tables.SITES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAINERPROFILES;
import static org.ccjmne.orca.jooq.codegen.Tables.USERS;
import static org.ccjmne.orca.jooq.codegen.Tables.USERS_ROLES;

import java.util.Random;

import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.jooq.codegen.tables.records.UsersRolesRecord;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.InsertValuesStep4;
import org.jooq.impl.DSL;

public class DemoDataUsers {

  // TODO: Inject all Randoms
  private static final Random RANDOM = new Random();

  private static final Field<String> GENERATED_PASSWORD = DSL.md5(DemoBareWorkingState.DEMO_PASSWORD);

  public static void generate(final DSLContext ctx) {
    ctx.selectFrom(EMPLOYEES).where(EMPLOYEES.EMPL_PK.ne(Constants.EMPLOYEE_ROOT)).orderBy(DSL.rand()).limit(30).forEach(empl -> {
      ctx.insertInto(USERS, USERS.USER_ID, USERS.USER_TYPE, USERS.USER_EMPL_FK, USERS.USER_PWD)
          .values(FakeRecords.asFields(empl.getEmplExternalId(), Constants.USERTYPE_EMPLOYEE, empl.getEmplPk(), GENERATED_PASSWORD))
          .execute();

      final int access = RANDOM.nextInt(3) + 2; // between [2, 4]
      try (InsertValuesStep4<UsersRolesRecord, String, String, Integer, Integer> roles = ctx
          .insertInto(USERS_ROLES, USERS_ROLES.USER_ID, USERS_ROLES.USRO_TYPE, USERS_ROLES.USRO_LEVEL, USERS_ROLES.USRO_TRPR_FK)
          .values(empl.getEmplExternalId(), Constants.ROLE_USER, null, null)
          .values(empl.getEmplExternalId(), Constants.ROLE_ACCESS, Integer.valueOf(access), null)) {

        if (access == 4) {
          roles.values(FakeRecords.asFields(empl.getEmplExternalId(), Constants.ROLE_TRAINER, null,
                                            FakeRecords.random(TRAINERPROFILES, TRAINERPROFILES.TRPR_PK)));
        }

        if (RANDOM.nextInt(3) == 0) {
          roles.values(empl.getEmplExternalId(), Constants.ROLE_ADMIN, Integer.valueOf(java.lang.Math.max(1, RANDOM.nextInt(access))), null);
        }

        roles.execute();
      }
    });

    ctx.selectFrom(SITES).where(SITES.SITE_PK.ne(Constants.DECOMMISSIONED_SITE)).orderBy(DSL.rand()).limit(10).forEach(site -> {
      ctx.insertInto(USERS, USERS.USER_ID, USERS.USER_TYPE, USERS.USER_SITE_FK, USERS.USER_PWD)
          .values(FakeRecords.asFields(site.getSiteExternalId(), Constants.USERTYPE_SITE, site.getSitePk(), GENERATED_PASSWORD))
          .execute();

      final int access = RANDOM.nextInt(3) + 2; // between [2, 4]
      try (InsertValuesStep4<UsersRolesRecord, String, String, Integer, Integer> roles = ctx
          .insertInto(USERS_ROLES, USERS_ROLES.USER_ID, USERS_ROLES.USRO_TYPE, USERS_ROLES.USRO_LEVEL, USERS_ROLES.USRO_TRPR_FK)
          .values(site.getSiteExternalId(), Constants.ROLE_ACCESS, Integer.valueOf(access), null)) {

        if (access == 4) {
          roles.values(FakeRecords.asFields(site.getSiteExternalId(), Constants.ROLE_TRAINER, null,
                                            FakeRecords.random(TRAINERPROFILES, TRAINERPROFILES.TRPR_PK)));
        }

        if (RANDOM.nextInt(3) == 0) {
          roles.values(site.getSiteExternalId(), Constants.ROLE_ADMIN, Integer.valueOf(1), null);
        }

        roles.execute();
      }
    });
  }
}
