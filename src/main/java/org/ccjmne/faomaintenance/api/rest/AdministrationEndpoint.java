package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.DEPARTMENTS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAINERPROFILES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAINERPROFILES_TRAININGTYPES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.USERS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.USERS_ROLES;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;

import org.ccjmne.faomaintenance.api.modules.Restrictions;
import org.ccjmne.faomaintenance.api.utils.Constants;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.UsersRolesRecord;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JoinType;
import org.jooq.Record;
import org.jooq.RecordMapper;
import org.jooq.Row1;
import org.jooq.Row2;
import org.jooq.SelectQuery;
import org.jooq.impl.DSL;

@Path("admin")
public class AdministrationEndpoint {

	private static final String ALPHANUMERIC = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
	private static final int PASSWORD_LENGTH = 8;
	private static final Random RANDOM = new Random();

	private final DSLContext ctx;

	@Inject
	public AdministrationEndpoint(final DSLContext ctx, final Restrictions restrictions) {
		this.ctx = ctx;
		if (!restrictions.canManageUsers()) {
			throw new ForbiddenException();
		}
	}

	@GET
	@Path("users")
	public List<Map<String, Object>> getUsers() {
		try (final SelectQuery<Record> query = this.ctx.selectQuery()) {
			query.addSelect(Constants.USERS_FIELDS);
			query.addSelect(EMPLOYEES.fields());
			query.addSelect(SITES.fields());
			query.addSelect(DEPARTMENTS.fields());
			query.addGroupBy(Constants.USERS_FIELDS);
			query.addGroupBy(EMPLOYEES.fields());
			query.addGroupBy(SITES.fields());
			query.addGroupBy(DEPARTMENTS.fields());
			query.addFrom(USERS);
			query.addSelect(
							DSL.arrayAgg(USERS_ROLES.USRO_TYPE).as("rolesTypes"),
							DSL.arrayAgg(USERS_ROLES.USRO_LEVEL).as("rolesLevels"),
							DSL.arrayAgg(USERS_ROLES.USRO_TRPR_FK).as("rolesTrprFks"));
			query.addJoin(USERS_ROLES, USERS_ROLES.USER_ID.eq(USERS.USER_ID));
			query.addJoin(EMPLOYEES, JoinType.FULL_OUTER_JOIN, EMPLOYEES.EMPL_PK.eq(USERS.USER_EMPL_FK));
			query.addJoin(SITES, JoinType.FULL_OUTER_JOIN, SITES.SITE_PK.eq(USERS.USER_SITE_FK));
			query.addJoin(DEPARTMENTS, JoinType.FULL_OUTER_JOIN, DEPARTMENTS.DEPT_PK.eq(USERS.USER_DEPT_FK));
			query.addConditions(USERS.USER_ID.ne(Constants.USER_ROOT));
			final List<Map<String, Object>> users = query.fetchMaps();

			for (final Map<String, Object> user : users) {
				final String[] rolesTypes = (String[]) user.remove("rolesTypes");
				final Integer[] rolesLevels = (Integer[]) user.remove("rolesLevels");
				final Integer[] rolesTrprFks = (Integer[]) user.remove("rolesTrprFks");
				if (rolesTypes.length > 0) {
					final Map<String, Object> roles = new HashMap<>();
					for (int i = 0; i < rolesTypes.length; i++) {
						switch (rolesTypes[i]) {
							case Constants.ROLE_ACCESS:
							case Constants.ROLE_ADMIN:
								roles.put(rolesTypes[i], rolesLevels[i]);
								break;
							case Constants.ROLE_TRAINER:
								roles.put(rolesTypes[i], rolesTrprFks[i]);
								break;
							default:
								roles.put(rolesTypes[i], Boolean.TRUE);
						}
					}

					user.put("roles", roles);
				}

			}

			return users;
		}
	}

	@GET
	@Path("users/{user_id}")
	public Map<String, Object> getUserInfo(@PathParam("user_id") final String user_id) {
		return AdministrationEndpoint.getUserInfoImpl(user_id, this.ctx);
	}

	/**
	 * Not part of the exposed API. Used by {@link AccountEndpoint} only.<br />
	 * Return account information and corresponding {@link Restrictions} for a
	 * given user ID.
	 */
	public static Map<String, Object> getUserInfoImpl(final String user_id, final DSLContext ctx) {
		final Map<String, Object> res = ctx
				.select(Constants.USERS_FIELDS)
				.select(EMPLOYEES.fields())
				.select(SITES.fields())
				.select(DEPARTMENTS.fields())
				.from(USERS)
				.leftOuterJoin(EMPLOYEES).on(EMPLOYEES.EMPL_PK.eq(USERS.USER_EMPL_FK))
				.leftOuterJoin(SITES).on(SITES.SITE_PK.eq(USERS.USER_SITE_FK))
				.leftOuterJoin(DEPARTMENTS).on(DEPARTMENTS.DEPT_PK.eq(USERS.USER_DEPT_FK))
				.where(USERS.USER_ID.eq(user_id))

				.fetchOneMap();

		res.put(
				"roles",
				ctx.selectFrom(USERS_ROLES).where(USERS_ROLES.USER_ID.eq(user_id))
						.fetchMap(USERS_ROLES.USRO_TYPE, (RecordMapper<UsersRolesRecord, Object>) entry -> {
							switch (entry.getUsroType()) {
								case Constants.ROLE_ACCESS:
								case Constants.ROLE_ADMIN:
									return entry.getUsroLevel();
								case Constants.ROLE_TRAINER:
									return entry.getUsroTrprFk();
								default:
									return Boolean.TRUE;
							}
						}));

		res.put("restrictions", Restrictions.forUser(user_id, ctx));
		return res;
	}

	@POST
	@Path("users/{user_id}")
	@Consumes(MediaType.APPLICATION_JSON)
	public String createUser(@PathParam("user_id") final String user_id, final Map<String, Object> data) {
		if (this.ctx.fetchExists(USERS, USERS.USER_ID.eq(user_id).or(USERS.USER_ID.ne(Constants.USER_ROOT)))) {
			throw new IllegalArgumentException("The user '" + user_id + "' already exists.");
		}

		return insertUserImpl(user_id, data);
	}

	@PUT
	@Path("users/{user_id}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void updateUser(@PathParam("user_id") final String user_id, final Map<String, Object> data) {
		if (!this.ctx.fetchExists(USERS, USERS.USER_ID.eq(user_id).and(USERS.USER_ID.ne(Constants.USER_ROOT)))) {
			throw new IllegalArgumentException("The user '" + user_id + "' does not exist.");
		}

		insertUserImpl(user_id, data);
	}

	/**
	 * Updates (or creates) data for a user identified by its ID. Returns its
	 * newly generated password in case of a user's creation.
	 *
	 * @param id
	 *            the user's ID
	 * @param data
	 *            the data used to update or create the specified user
	 * @return <code>null</code> or a generated password if the user didn't
	 *         exist.
	 */
	@SuppressWarnings("unchecked")
	public String insertUserImpl(final String id, final Map<String, Object> data) {
		return this.ctx.transactionResult((config) -> {
			try (final DSLContext transactionCtx = DSL.using(config)) {
				final String password;
				if (!transactionCtx.fetchExists(USERS, USERS.USER_ID.eq(id))) {
					transactionCtx.insertInto(USERS, USERS.USER_ID, USERS.USER_PWD, USERS.USER_TYPE, USERS.USER_EMPL_FK, USERS.USER_SITE_FK, USERS.USER_DEPT_FK)
							.values(
									id,
									password = generatePassword(),
									(String) data.get(USERS.USER_TYPE.getName()),
									(String) data.get(USERS.USER_EMPL_FK.getName()),
									(String) data.get(USERS.USER_SITE_FK.getName()),
									(Integer) data.get(USERS.USER_DEPT_FK.getName()))
							.execute();
				} else {
					password = null;
					transactionCtx.update(USERS)
							.set(USERS.USER_TYPE, (String) data.get(USERS.USER_TYPE.getName()))
							.set(USERS.USER_EMPL_FK, (String) data.get(USERS.USER_EMPL_FK.getName()))
							.set(USERS.USER_SITE_FK, (String) data.get(USERS.USER_SITE_FK.getName()))
							.set(USERS.USER_DEPT_FK, (Integer) data.get(USERS.USER_DEPT_FK.getName())).execute();
				}

				transactionCtx.delete(USERS_ROLES).where(USERS_ROLES.USER_ID.eq(id)).execute();
				final Map<String, Object> roles = (Map<String, Object>) data.get("roles");
				if ((roles != null) && !roles.isEmpty()) {
					for (final String type : roles.keySet()) {
						final Field<Integer> specification;
						switch (type) {
							case Constants.ROLE_ACCESS:
								if (Constants.USERTYPE_DEPARTMENT.equals(data.get(USERS.USER_TYPE.getName())) && (((Integer) roles.get(type)).intValue() < 2)) {
									throw new IllegalArgumentException(
																		String.format(
																						"A user of type '%s' cannot be granted a role '%s' which level is lower than 2.",
																						Constants.USERTYPE_DEPARTMENT,
																						Constants.ROLE_ACCESS));
								}

								//$FALL-THROUGH$
							case Constants.ROLE_ADMIN:
								specification = USERS_ROLES.USRO_LEVEL;
								break;
							case Constants.ROLE_TRAINER:
								specification = USERS_ROLES.USRO_TRPR_FK;
								break;
							default:
								specification = null;
						}

						if (specification == null) {
							transactionCtx.insertInto(USERS_ROLES).set(USERS_ROLES.USER_ID, id).set(USERS_ROLES.USRO_TYPE, type).execute();
						} else {
							transactionCtx.insertInto(USERS_ROLES)
									.set(USERS_ROLES.USER_ID, id)
									.set(USERS_ROLES.USRO_TYPE, type)
									.set(specification, (Integer) roles.get(type)).execute();
						}
					}
				}

				return password;
			}
		});
	}

	@DELETE
	@Path("users/{user_id}")
	public boolean delete(@PathParam("user_id") final String user_id) {
		return this.ctx.delete(USERS).where(USERS.USER_ID.eq(user_id)).execute() > 0;
	}

	@DELETE
	@Path("users/{user_id}/password")
	public String resetPassword(@PathParam("user_id") final String user_id) {
		final String password = generatePassword();
		this.ctx.update(USERS).set(USERS.USER_PWD, DSL.md5(password)).where(USERS.USER_ID.eq(user_id)).execute();
		return password;
	}

	@GET
	@Path("trainerprofiles")
	public Map<Integer, ? extends Record> getTrainerprofiles() {
		return this.ctx.select(TRAINERPROFILES.TRPR_PK, TRAINERPROFILES.TRPR_ID, DSL.arrayAgg(TRAINERPROFILES_TRAININGTYPES.TPTT_TRTY_FK).as("types"))
				.from(TRAINERPROFILES).leftOuterJoin(TRAINERPROFILES_TRAININGTYPES).on(TRAINERPROFILES_TRAININGTYPES.TPTT_TRPR_FK.eq(TRAINERPROFILES.TRPR_PK))
				.groupBy(TRAINERPROFILES.TRPR_PK, TRAINERPROFILES.TRPR_ID).fetchMap(TRAINERPROFILES.TRPR_PK);
	}

	@POST
	@Path("trainerprofiles")
	@Consumes(MediaType.APPLICATION_JSON)
	@SuppressWarnings("unchecked")
	public Integer createTrainerprofile(final Map<String, Object> level) {
		return this.ctx.transactionResult((config) -> {
			try (final DSLContext transactionCtx = DSL.using(config)) {
				final Integer trpr_pk = transactionCtx.select(DSL.max(TRAINERPROFILES.TRPR_PK).add(Integer.valueOf(1)).as(TRAINERPROFILES.TRPR_PK.getName()))
						.from(TRAINERPROFILES).fetchOne(TRAINERPROFILES.TRPR_PK.getName(), Integer.class);
				transactionCtx.insertInto(TRAINERPROFILES, TRAINERPROFILES.TRPR_PK, TRAINERPROFILES.TRPR_ID)
						.values(trpr_pk, (String) level.get(TRAINERPROFILES.TRPR_ID.getName())).execute();
				insertTypes(trpr_pk, (List<Integer>) level.get("types"), transactionCtx);
				return trpr_pk;
			}
		});
	}

	@PUT
	@Path("trainerprofiles/{trpr_pk}")
	@Consumes(MediaType.APPLICATION_JSON)
	@SuppressWarnings("unchecked")
	public void updateTrainerprofile(@PathParam("trpr_pk") final Integer trpr_pk, final Map<String, Object> level) {
		this.ctx.transaction((config) -> {
			try (final DSLContext transactionCtx = DSL.using(config)) {
				transactionCtx.update(TRAINERPROFILES).set(TRAINERPROFILES.TRPR_ID, (String) level.get(TRAINERPROFILES.TRPR_PK.getName()))
						.where(TRAINERPROFILES.TRPR_PK.eq(trpr_pk)).execute();
				insertTypes(trpr_pk, (List<Integer>) level.get("types"), transactionCtx);
			}
		});
	}

	@DELETE
	@Path("trainerprofiles/{trpr_pk}")
	public boolean deleteTrainerprofile(@PathParam("trpr_pk") final Integer trpr_pk) {
		return this.ctx.delete(TRAINERPROFILES).where(TRAINERPROFILES.TRPR_PK.eq(trpr_pk)).execute() > 0;
	}

	private static String generatePassword() {
		final char[] res = new char[PASSWORD_LENGTH];
		for (int i = 0; i < PASSWORD_LENGTH; i++) {
			res[i] = ALPHANUMERIC.charAt(RANDOM.nextInt(ALPHANUMERIC.length()));
		}

		return String.valueOf(res);
	}

	@SuppressWarnings("unchecked")
	private static void insertTypes(final Integer trpr_pk, final List<Integer> types, final DSLContext transactionCtx) {
		if (Constants.UNASSIGNED_TRAINERPROFILE.equals(trpr_pk)) {
			return;
		}

		transactionCtx.delete(TRAINERPROFILES_TRAININGTYPES).where(TRAINERPROFILES_TRAININGTYPES.TPTT_TRPR_FK.eq(trpr_pk)).execute();
		if (!types.isEmpty()) {
			final List<Row1<Integer>> rows = new ArrayList<>(types.size());
			types.forEach(type -> rows.add(DSL.row(type)));
			transactionCtx.insertInto(TRAINERPROFILES_TRAININGTYPES, TRAINERPROFILES_TRAININGTYPES.TPTT_TRPR_FK, TRAINERPROFILES_TRAININGTYPES.TPTT_TRTY_FK)
					.select(DSL.select(DSL.val(trpr_pk), DSL.field(TRAINERPROFILES_TRAININGTYPES.TPTT_TRTY_FK.getName(), Integer.class))
							.from(DSL.values(rows.toArray(new Row2[0])).as("unused", TRAINERPROFILES_TRAININGTYPES.TPTT_TRTY_FK.getName())))
					.execute();
		}
	}
}
