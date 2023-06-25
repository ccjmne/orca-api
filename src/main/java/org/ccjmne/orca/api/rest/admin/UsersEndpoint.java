package org.ccjmne.orca.api.rest.admin;

import static org.ccjmne.orca.jooq.codegen.Tables.EMPLOYEES;
import static org.ccjmne.orca.jooq.codegen.Tables.SITES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAINERPROFILES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAINERPROFILES_TRAININGTYPES;
import static org.ccjmne.orca.jooq.codegen.Tables.USERS;
import static org.ccjmne.orca.jooq.codegen.Tables.USERS_ROLES;

import java.util.ArrayList;
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

import org.ccjmne.orca.api.modules.Restrictions;
import org.ccjmne.orca.api.rest.utils.AccountEndpoint;
import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.api.utils.ResourcesHelper;
import org.ccjmne.orca.api.utils.Transactions;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Row1;
import org.jooq.Row2;
import org.jooq.impl.DSL;

import com.google.common.collect.ObjectArrays;

@Path("users-admin")
public class UsersEndpoint {

	private static final String ALPHANUMERIC = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
	private static final int PASSWORD_LENGTH = 8;
	private static final Random RANDOM = new Random();

	private final DSLContext ctx;

	@Inject
	public UsersEndpoint(final DSLContext ctx, final Restrictions restrictions) {
		this.ctx = ctx;
		if (!restrictions.canManageUsers()) {
			throw new ForbiddenException();
		}
	}

	@GET
	@Path("users")
	@SuppressWarnings("null")
	public List<Map<String, Object>> getUsers() {
		return this.ctx.select(Constants.USERS_FIELDS).select(EMPLOYEES.fields()).select(SITES.fields())
				.select(DSL.arrayAgg(USERS_ROLES.USRO_TYPE).filterWhere(USERS_ROLES.USRO_TYPE.isNotNull()).as("type_array"),
						DSL.arrayAgg(USERS_ROLES.USRO_LEVEL).filterWhere(USERS_ROLES.USRO_TYPE.isNotNull()).as("level_array"),
						DSL.arrayAgg(USERS_ROLES.USRO_TRPR_FK).filterWhere(USERS_ROLES.USRO_TYPE.isNotNull()).as("trainer_array"),
						DSL.arrayAgg(DSL.when(USERS_ROLES.USRO_TYPE.isNotNull(), Boolean.TRUE)).as("true_array"))
				.from(USERS)
				.leftOuterJoin(EMPLOYEES).on(EMPLOYEES.EMPL_PK.eq(USERS.USER_EMPL_FK))
				.leftOuterJoin(SITES).on(SITES.SITE_PK.eq(USERS.USER_SITE_FK))
				.leftOuterJoin(USERS_ROLES).on(USERS_ROLES.USER_ID.eq(USERS.USER_ID))
				.where(USERS.USER_ID.ne(Constants.USER_ROOT))
				.groupBy(ObjectArrays.concat(ObjectArrays.concat(Constants.USERS_FIELDS, EMPLOYEES.fields(), Field.class), SITES.fields(), Field.class))
				.fetch(ResourcesHelper.getMapperWithZip(ResourcesHelper
						.getZipSelectMapper("type_array", "level_array", "trainer_array", "true_array"), "roles"));
	}

	@GET
	@Path("users/{user_id}")
	public Map<String, Object> getUserInfo(@PathParam("user_id") final String user_id) {
		return UsersEndpoint.getUserInfoImpl(user_id, this.ctx);
	}

	/**
	 * Not part of the exposed API. Used by {@link AccountEndpoint} only.<br />
	 * Returns account information and corresponding {@link Restrictions} for a
	 * given user ID.
	 */
	@SuppressWarnings("null")
	public static Map<String, Object> getUserInfoImpl(final String user_id, final DSLContext ctx) {
		final Map<String, Object> res = ctx.select(Constants.USERS_FIELDS).select(EMPLOYEES.fields()).select(SITES.fields())
				.select(DSL.arrayAgg(USERS_ROLES.USRO_TYPE).filterWhere(USERS_ROLES.USRO_TYPE.isNotNull()).as("type_array"),
						DSL.arrayAgg(USERS_ROLES.USRO_LEVEL).filterWhere(USERS_ROLES.USRO_TYPE.isNotNull()).as("level_array"),
						DSL.arrayAgg(USERS_ROLES.USRO_TRPR_FK).filterWhere(USERS_ROLES.USRO_TYPE.isNotNull()).as("trainer_array"),
						DSL.arrayAgg(DSL.when(USERS_ROLES.USRO_TYPE.isNotNull(), Boolean.TRUE)).as("true_array"))
				.from(USERS)
				.leftOuterJoin(EMPLOYEES).on(EMPLOYEES.EMPL_PK.eq(USERS.USER_EMPL_FK))
				.leftOuterJoin(SITES).on(SITES.SITE_PK.eq(USERS.USER_SITE_FK))
				.leftOuterJoin(USERS_ROLES).on(USERS_ROLES.USER_ID.eq(USERS.USER_ID))
				.where(USERS.USER_ID.eq(user_id))
				.groupBy(ObjectArrays.concat(ObjectArrays.concat(Constants.USERS_FIELDS, EMPLOYEES.fields(), Field.class), SITES.fields(), Field.class))
				.fetchOne(ResourcesHelper.getMapperWithZip(ResourcesHelper
						.getZipSelectMapper("type_array", "level_array", "trainer_array", "true_array"), "roles"));

		res.put("restrictions", Restrictions.forUser(user_id, ctx));
		return res;
	}

	@POST
	@Path("users/{user_id}")
	@Consumes(MediaType.APPLICATION_JSON)
	public String createUser(@PathParam("user_id") final String user_id, final Map<String, Object> data) {
		if (this.ctx.fetchExists(USERS, USERS.USER_ID.eq(user_id))) {
			throw new IllegalArgumentException(String.format("The user '%s' already exists.", user_id));
		}

		return this.insertUserImpl(user_id, data);
	}

	@PUT
	@Path("users/{user_id}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void updateUser(@PathParam("user_id") final String user_id, final Map<String, Object> data) {
		if (!this.ctx.fetchExists(USERS, USERS.USER_ID.eq(user_id).and(USERS.USER_ID.ne(Constants.USER_ROOT)))) {
			throw new IllegalArgumentException(String.format("The user '%s' does not exist.", user_id));
		}

		this.insertUserImpl(user_id, data);
	}

	/**
	 * Updates (or creates) data for a user identified by its ID. Returns its
	 * newly generated password in case of a user's creation.
	 *
	 * @param user_id
	 *            the user's ID
	 * @param data
	 *            the data used to update or create the specified user
	 * @return <code>null</code> or a generated password if the user didn't
	 *         exist.
	 */
	@SuppressWarnings("unchecked")
	public String insertUserImpl(final String user_id, final Map<String, Object> data) {
		return Transactions.with(this.ctx, transactionCtx -> {
			final String password;
			if (!transactionCtx.fetchExists(USERS, USERS.USER_ID.eq(user_id))) {
				transactionCtx.insertInto(USERS, USERS.USER_ID, USERS.USER_PWD, USERS.USER_TYPE, USERS.USER_EMPL_FK, USERS.USER_SITE_FK)
						.values(
								DSL.val(user_id),
								DSL.md5(password = UsersEndpoint.generatePassword()),
								DSL.val((String) data.get(USERS.USER_TYPE.getName())),
								DSL.val((Integer) data.get(USERS.USER_EMPL_FK.getName())),
								DSL.val((Integer) data.get(USERS.USER_SITE_FK.getName())))
						.execute();
			} else {
				password = null;
				transactionCtx.update(USERS)
						.set(USERS.USER_TYPE, (String) data.get(USERS.USER_TYPE.getName()))
						.set(USERS.USER_EMPL_FK, (Integer) data.get(USERS.USER_EMPL_FK.getName()))
						.set(USERS.USER_SITE_FK, (Integer) data.get(USERS.USER_SITE_FK.getName()))
						.where(USERS.USER_ID.eq(user_id)).execute();
			}

			transactionCtx.delete(USERS_ROLES).where(USERS_ROLES.USER_ID.eq(user_id)).execute();
			final Map<String, Object> roles = (Map<String, Object>) data.get("roles");
			if ((roles != null) && !roles.isEmpty()) {
				for (final String type : roles.keySet()) {
					final Field<Integer> specification;
					switch (type) {
						case Constants.ROLE_ACCESS:
							if (Constants.ACCESS_LEVEL_ONESELF.equals(roles.get(type))) {
								throw new IllegalArgumentException(String
										.format("Cannot create users with Access level '%d' in the current version.", Constants.ACCESS_LEVEL_ONESELF));
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
						transactionCtx.insertInto(USERS_ROLES).set(USERS_ROLES.USER_ID, user_id).set(USERS_ROLES.USRO_TYPE, type).execute();
					} else {
						transactionCtx.insertInto(USERS_ROLES)
								.set(USERS_ROLES.USER_ID, user_id)
								.set(USERS_ROLES.USRO_TYPE, type)
								.set(specification, (Integer) roles.get(type)).execute();
					}
				}
			}

			return password;
		});
	}

	@PUT
	@Path("users/{user_id}/{new_id}")
	public void changeId(@PathParam("user_id") final String user_id, @PathParam("new_id") final String newId) {
		UsersEndpoint.changeIdImpl(user_id, newId, this.ctx);
	}

	/**
	 * Not part of the exposed API. Used by {@link AccountEndpoint} only.
	 */
	public static void changeIdImpl(final String user_id, final String newId, final DSLContext ctx) {
		Transactions.with(ctx, transactionCtx -> {
			if (transactionCtx.fetchExists(USERS, USERS.USER_ID.ne(user_id).and(USERS.USER_ID.eq(newId)))) {
				throw new IllegalArgumentException(String.format("The ID '%s' is already attributed to another user and thus is not available.", newId));
			}

			if (0 == transactionCtx.update(USERS).set(USERS.USER_ID, newId).where(USERS.USER_ID.eq(user_id)).execute()) {
				throw new IllegalArgumentException(String.format("The user '%s' does not exist.", user_id));
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
		final String password = UsersEndpoint.generatePassword();
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
	@SuppressWarnings({ "unchecked", "null" })
	public Integer createTrainerprofile(final Map<String, Object> level) {
		return Transactions.with(this.ctx, transactionCtx -> {
			final Integer trpr_pk = transactionCtx.select(DSL.max(TRAINERPROFILES.TRPR_PK).add(Integer.valueOf(1)).as(TRAINERPROFILES.TRPR_PK.getName()))
					.from(TRAINERPROFILES).fetchOne(TRAINERPROFILES.TRPR_PK.getName(), Integer.class);
			transactionCtx.insertInto(TRAINERPROFILES, TRAINERPROFILES.TRPR_PK, TRAINERPROFILES.TRPR_ID)
					.values(trpr_pk, (String) level.get(TRAINERPROFILES.TRPR_ID.getName())).execute();
			UsersEndpoint.insertTypes(trpr_pk, (List<Integer>) level.get("types"), transactionCtx);
			return trpr_pk;
		});
	}

	@PUT
	@Path("trainerprofiles/{trpr_pk}")
	@Consumes(MediaType.APPLICATION_JSON)
	@SuppressWarnings("unchecked")
	public void updateTrainerprofile(@PathParam("trpr_pk") final Integer trpr_pk, final Map<String, Object> level) {
		Transactions.with(this.ctx, transactionCtx -> {
			transactionCtx.update(TRAINERPROFILES).set(TRAINERPROFILES.TRPR_ID, (String) level.get(TRAINERPROFILES.TRPR_ID.getName()))
					.where(TRAINERPROFILES.TRPR_PK.eq(trpr_pk)).execute();
			UsersEndpoint.insertTypes(trpr_pk, (List<Integer>) level.get("types"), transactionCtx);
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
		if (Constants.DEFAULT_TRAINERPROFILE.equals(trpr_pk)) {
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
