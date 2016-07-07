package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES_ROLES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAINERPROFILES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAINERPROFILES_TRAININGTYPES;

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
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Row1;
import org.jooq.Row2;
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
		final List<Map<String, Object>> users = this.ctx.select(
																EMPLOYEES.EMPL_PK,
																EMPLOYEES.EMPL_FIRSTNAME,
																EMPLOYEES.EMPL_SURNAME,
																EMPLOYEES.EMPL_DOB,
																EMPLOYEES.EMPL_PERMANENT,
																EMPLOYEES.EMPL_GENDER,
																EMPLOYEES.EMPL_NOTES,
																EMPLOYEES.EMPL_ADDR,
																DSL.arrayAgg(EMPLOYEES_ROLES.EMRO_TYPE).as("rolesTypes"),
																DSL.arrayAgg(EMPLOYEES_ROLES.EMRO_LEVEL).as("rolesLevels"),
																DSL.arrayAgg(EMPLOYEES_ROLES.EMRO_TRPR_FK).as("rolesTrprFks"))
				.from(EMPLOYEES).join(EMPLOYEES_ROLES).on(EMPLOYEES_ROLES.EMPL_PK.eq(EMPLOYEES.EMPL_PK))
				.where(EMPLOYEES.EMPL_PWD.isNotNull())
				.and(EMPLOYEES.EMPL_PK.ne(Constants.USER_ROOT))
				.groupBy(
							EMPLOYEES.EMPL_PK,
							EMPLOYEES.EMPL_FIRSTNAME,
							EMPLOYEES.EMPL_SURNAME,
							EMPLOYEES.EMPL_DOB,
							EMPLOYEES.EMPL_PERMANENT,
							EMPLOYEES.EMPL_GENDER,
							EMPLOYEES.EMPL_NOTES,
							EMPLOYEES.EMPL_ADDR)
				.fetchMaps();

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

	@GET
	@Path("users/{empl_pk}")
	public Map<String, Object> getUserInfo(@PathParam("empl_pk") final String empl_pk) {
		return AdministrationEndpoint.getUserInfoImpl(empl_pk, this.ctx);
	}

	/**
	 * Not part of the exposed API. Used by {@link AccountEndpoint} only.<br />
	 * Return account information and corresponding {@link Restrictions} for a
	 * given user ID.
	 */
	public static Map<String, Object> getUserInfoImpl(final String empl_pk, final DSLContext ctx) {
		final Map<String, Object> res = ctx
				.select(
						EMPLOYEES.EMPL_PK,
						EMPLOYEES.EMPL_FIRSTNAME,
						EMPLOYEES.EMPL_SURNAME,
						EMPLOYEES.EMPL_DOB,
						EMPLOYEES.EMPL_PERMANENT,
						EMPLOYEES.EMPL_GENDER,
						EMPLOYEES.EMPL_NOTES,
						EMPLOYEES.EMPL_ADDR)
				.from(EMPLOYEES).where(EMPLOYEES.EMPL_PK.eq(empl_pk))
				.fetchOneMap();
		res.put(
				"roles",
				ctx.selectFrom(EMPLOYEES_ROLES).where(EMPLOYEES_ROLES.EMPL_PK.eq(empl_pk))
						.fetchMap(EMPLOYEES_ROLES.EMRO_TYPE, Constants.EMPLOYEES_ROLES_MAPPER));
		res.put("restrictions", Restrictions.forEmployee(empl_pk, ctx));
		return res;
	}

	@POST
	@Path("users/{empl_pk}")
	@Consumes(MediaType.APPLICATION_JSON)
	public String createUser(@PathParam("empl_pk") final String empl_pk, final Map<String, Object> roles) {
		final String password = resetPassword(empl_pk);
		updateUser(empl_pk, roles);
		return password;
	}

	@PUT
	@Path("users/{empl_pk}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void updateUser(@PathParam("empl_pk") final String empl_pk, final Map<String, Object> roles) {
		this.ctx.transaction((config) -> {
			try (final DSLContext transactionCtx = DSL.using(config)) {
				if (!transactionCtx.fetchExists(EMPLOYEES, EMPLOYEES.EMPL_PK.eq(empl_pk).and(EMPLOYEES.EMPL_PWD.isNotNull()))) {
					throw new IllegalArgumentException("The user '" + empl_pk + "' does not have a password and therefore cannot be assigned any role.");
				}

				transactionCtx.delete(EMPLOYEES_ROLES).where(EMPLOYEES_ROLES.EMPL_PK.eq(empl_pk)).execute();
				if (!roles.isEmpty()) {
					for (final String type : roles.keySet()) {
						final Field<Integer> specification = getRoleSpecificationField(type);
						if (specification == null) {
							transactionCtx.insertInto(EMPLOYEES_ROLES).set(EMPLOYEES_ROLES.EMPL_PK, empl_pk).set(EMPLOYEES_ROLES.EMRO_TYPE, type).execute();
						} else {
							transactionCtx.insertInto(EMPLOYEES_ROLES).set(EMPLOYEES_ROLES.EMPL_PK, empl_pk).set(EMPLOYEES_ROLES.EMRO_TYPE, type)
									.set(specification, (Integer) roles.get(type)).execute();
						}
					}
				}
			}
		});
	}

	@DELETE
	@Path("users/{empl_pk}")
	public boolean delete(@PathParam("empl_pk") final String empl_pk) {
		return this.ctx.delete(EMPLOYEES_ROLES).where(EMPLOYEES_ROLES.EMPL_PK.eq(empl_pk)).execute() > 0;
	}

	@DELETE
	@Path("users/{empl_pk}/password")
	public String resetPassword(@PathParam("empl_pk") final String empl_pk) {
		final String password = generatePassword();
		this.ctx.update(EMPLOYEES).set(EMPLOYEES.EMPL_PWD, password).where(EMPLOYEES.EMPL_PK.eq(empl_pk)).execute();
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
				final Integer trlv_pk = transactionCtx.select(DSL.max(TRAINERPROFILES.TRPR_PK).add(Integer.valueOf(1)).as(TRAINERPROFILES.TRPR_PK.getName()))
						.from(TRAINERPROFILES).fetchOne(TRAINERPROFILES.TRPR_PK.getName(), Integer.class);
				transactionCtx.insertInto(TRAINERPROFILES, TRAINERPROFILES.TRPR_PK, TRAINERPROFILES.TRPR_ID)
						.values(trlv_pk, (String) level.get(TRAINERPROFILES.TRPR_ID.getName())).execute();
				insertTypes(trlv_pk, (List<Integer>) level.get("types"), transactionCtx);
				return trlv_pk;
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

	private static Field<Integer> getRoleSpecificationField(final String type) {
		switch (type) {
			case Constants.ROLE_ACCESS:
			case Constants.ROLE_ADMIN:
				return EMPLOYEES_ROLES.EMRO_LEVEL;
			case Constants.ROLE_TRAINER:
				return EMPLOYEES_ROLES.EMRO_TRPR_FK;
			default:
				return null;
		}
	}

	@SuppressWarnings("unchecked")
	private static void insertTypes(final Integer trlv_pk, final List<Integer> types, final DSLContext transactionCtx) {
		if (Constants.UNASSIGNED_TRAINERPROFILE.equals(trlv_pk)) {
			return;
		}

		transactionCtx.delete(TRAINERPROFILES_TRAININGTYPES).where(TRAINERPROFILES_TRAININGTYPES.TPTT_TRPR_FK.eq(trlv_pk)).execute();
		if (!types.isEmpty()) {
			final List<Row1<Integer>> rows = new ArrayList<>(types.size());
			types.forEach(type -> rows.add(DSL.row(type)));
			transactionCtx.insertInto(TRAINERPROFILES_TRAININGTYPES, TRAINERPROFILES_TRAININGTYPES.TPTT_TRPR_FK, TRAINERPROFILES_TRAININGTYPES.TPTT_TRTY_FK)
					.select(DSL.select(DSL.val(trlv_pk), DSL.field(TRAINERPROFILES_TRAININGTYPES.TPTT_TRTY_FK.getName(), Integer.class))
							.from(DSL.values(rows.toArray(new Row2[0])).as("unused", TRAINERPROFILES_TRAININGTYPES.TPTT_TRTY_FK.getName())))
					.execute();
		}
	}
}
