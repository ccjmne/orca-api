package org.ccjmne.faomaintenance.api.demo;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAINERPROFILES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.USERS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.USERS_ROLES;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.ccjmne.faomaintenance.api.utils.Constants;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.UsersRolesRecord;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.InsertValuesStep4;
import org.jooq.Table;
import org.jooq.impl.DSL;

public class DemoDataUsers {

	private static final Random RANDOM = new Random();
	private static final Field<String> GENERATED_PASSWORD = DSL.md5("");

	public static void generate(final DSLContext ctx) {
		ctx.selectFrom(EMPLOYEES).orderBy(DSL.rand()).limit(30).fetch(EMPLOYEES.EMPL_PK).forEach(empl_pk -> {
			ctx.insertInto(USERS, USERS.USER_ID, USERS.USER_TYPE, USERS.USER_EMPL_FK, USERS.USER_PWD)
					.values(asFields(empl_pk, Constants.USERTYPE_EMPLOYEE, empl_pk, GENERATED_PASSWORD)).execute();

			try (InsertValuesStep4<UsersRolesRecord, String, String, Integer, Integer> roles = ctx
					.insertInto(USERS_ROLES, USERS_ROLES.USER_ID, USERS_ROLES.USRO_TYPE, USERS_ROLES.USRO_LEVEL, USERS_ROLES.USRO_TRPR_FK)
					.values(empl_pk, Constants.ROLE_USER, null, null)
					.values(empl_pk, Constants.ROLE_ACCESS, Integer.valueOf(RANDOM.nextInt(4) + 1), null)) {

				if (RANDOM.nextInt(3) == 0) {
					roles.values(asFields(empl_pk, Constants.ROLE_TRAINER, null, random(TRAINERPROFILES, TRAINERPROFILES.TRPR_PK)));
				}

				if (RANDOM.nextInt(3) == 0) {
					roles.values(empl_pk, Constants.ROLE_ADMIN, Integer.valueOf(RANDOM.nextInt(4) + 1), null);
				}

				roles.execute();
			}
		});

		ctx.selectFrom(SITES).orderBy(DSL.rand()).limit(10).fetch(SITES.SITE_PK).forEach(site_pk -> {
			ctx.insertInto(USERS, USERS.USER_ID, USERS.USER_TYPE, USERS.USER_SITE_FK, USERS.USER_PWD)
					.values(asFields(site_pk, Constants.USERTYPE_SITE, site_pk, GENERATED_PASSWORD)).execute();

			try (InsertValuesStep4<UsersRolesRecord, String, String, Integer, Integer> roles = ctx
					.insertInto(USERS_ROLES, USERS_ROLES.USER_ID, USERS_ROLES.USRO_TYPE, USERS_ROLES.USRO_LEVEL, USERS_ROLES.USRO_TRPR_FK)
					.values(site_pk, Constants.ROLE_ACCESS, Integer.valueOf(RANDOM.nextInt(4) + 1), null)) {

				if (RANDOM.nextInt(3) == 0) {
					roles.values(asFields(site_pk, Constants.ROLE_TRAINER, null, random(TRAINERPROFILES, TRAINERPROFILES.TRPR_PK)));
				}
				if (RANDOM.nextInt(3) == 0) {
					roles.values(site_pk, Constants.ROLE_ADMIN, Integer.valueOf(1), null);
				}

				roles.execute();
			}
		});
	}

	private static <R> Field<R> random(final Table<?> table, final Field<R> field, final Condition... conditions) {
		return DSL.select(field).from(table).where(conditions).orderBy(DSL.rand()).limit(1).asField();
	}

	private static List<? extends Field<?>> asFields(final Object... values) {
		return Arrays.asList(values).stream().map(v -> v instanceof Field<?> ? (Field<?>) v : DSL.field("?", v)).collect(Collectors.toList());
	}
}
