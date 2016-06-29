package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES_ROLES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAINERLEVELS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAINERLEVELS_TRAININGTYPES;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
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
import org.jooq.Result;
import org.jooq.Row1;
import org.jooq.impl.DSL;

@SuppressWarnings("unchecked")
@Path("admin")
public class AdministrationEndpoint {

	private static final String ALPHANUMERIC = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
	private static final int PASSWORD_LENGTH = 8;
	private static final Random RANDOM = new Random();

	private final DSLContext ctx;

	@Inject
	public AdministrationEndpoint(final DSLContext ctx) {
		this.ctx = ctx;
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
																DSL.arrayAgg(EMPLOYEES_ROLES.EMRO_TRLV_FK).as("rolesTrlvPks"))
				.from(EMPLOYEES).join(EMPLOYEES_ROLES).on(EMPLOYEES_ROLES.EMPL_PK.eq(EMPLOYEES.EMPL_PK))
				.where(EMPLOYEES.EMPL_PWD.isNotNull())
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
			final Integer[] rolesTrlvPks = (Integer[]) user.remove("rolesTrlvPks");
			if (rolesTypes.length > 0) {
				final Map<String, Object> roles = new HashMap<>();
				for (int i = 0; i < rolesTypes.length; i++) {
					switch (rolesTypes[i]) {
						case Constants.ROLE_ACCESS:
						case Constants.ROLE_ADMIN:
							roles.put(rolesTypes[i], rolesLevels[i]);
							break;
						case Constants.ROLE_TRAINER:
							roles.put(rolesTypes[i], rolesTrlvPks[i]);
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
		final Map<String, Object> res = this.ctx
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
				this.ctx.selectFrom(EMPLOYEES_ROLES).where(EMPLOYEES_ROLES.EMPL_PK.eq(empl_pk))
						.fetchMap(EMPLOYEES_ROLES.EMRO_TYPE, Constants.EMPLOYEES_ROLES_MAPPER));
		res.put("restrictions", Restrictions.forEmployee(empl_pk, this.ctx));
		return res;
	}

	@PUT
	@Path("users/{empl_pk}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void updateUser(@PathParam("empl_pk") final String empl_pk, final Map<String, Integer> roles) {
		this.ctx.transaction((config) -> {
			try (final DSLContext transactionCtx = DSL.using(config)) {
				transactionCtx.delete(EMPLOYEES_ROLES).where(EMPLOYEES_ROLES.EMPL_PK.eq(empl_pk));
				if (!roles.isEmpty()) {
					for (final String type : roles.keySet()) {
						final Field<Integer> specification = getRoleSpecificationField(type);
						if (specification != null) {
							transactionCtx.insertInto(EMPLOYEES_ROLES).set(EMPLOYEES_ROLES.EMPL_PK, empl_pk).set(EMPLOYEES_ROLES.EMRO_TYPE, type).execute();
						} else {
							transactionCtx.insertInto(EMPLOYEES_ROLES).set(EMPLOYEES_ROLES.EMPL_PK, empl_pk).set(EMPLOYEES_ROLES.EMRO_TYPE, type)
									.set(specification, roles.get(type)).execute();
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
	@Path("trainerlevels")
	public Result<? extends Record> getTrainerLevels() {
		return this.ctx.select(TRAINERLEVELS.TRLV_PK, TRAINERLEVELS.TRLV_ID, DSL.arrayAgg(TRAINERLEVELS_TRAININGTYPES.TLTR_TRTY_FK).as("types"))
				.from(TRAINERLEVELS).leftOuterJoin(TRAINERLEVELS_TRAININGTYPES).on(TRAINERLEVELS_TRAININGTYPES.TLTR_TRLV_FK.eq(TRAINERLEVELS.TRLV_PK))
				.groupBy(TRAINERLEVELS.TRLV_PK, TRAINERLEVELS.TRLV_ID).fetch();
	}

	@POST
	@Path("trainerlevels")
	@Consumes(MediaType.APPLICATION_JSON)
	public Integer createTrainerlevel(final Map<String, Object> level) {
		return this.ctx.transactionResult((config) -> {
			try (final DSLContext transactionCtx = DSL.using(config)) {
				final Integer trlv_pk = transactionCtx.select(DSL.max(TRAINERLEVELS.TRLV_PK).add(Integer.valueOf(1)).as("trlv_pk"))
						.from(TRAINERLEVELS).fetchOne("trlv_pk", Integer.class);
				transactionCtx.insertInto(TRAINERLEVELS, TRAINERLEVELS.TRLV_PK, TRAINERLEVELS.TRLV_ID).values(trlv_pk, (String) level.get("id")).execute();
				insertTypes(trlv_pk, (List<Integer>) level.get("types"), transactionCtx);
				return trlv_pk;
			}
		});
	}

	@PUT
	@Path("trainerlevels/{trlv_pk}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void updateTrainerlevel(@PathParam("trlv_pk") final Integer trlv_pk, final Map<String, Object> level) {
		this.ctx.transaction((config) -> {
			try (final DSLContext transactionCtx = DSL.using(config)) {
				transactionCtx.update(TRAINERLEVELS).set(TRAINERLEVELS.TRLV_ID, (String) level.get(TRAINERLEVELS.TRLV_ID.getName())).execute();
				transactionCtx.delete(TRAINERLEVELS_TRAININGTYPES).where(TRAINERLEVELS_TRAININGTYPES.TLTR_TRLV_FK.eq(trlv_pk));
				insertTypes(trlv_pk, (List<Integer>) level.get("types"), transactionCtx);
			}
		});
	}

	@DELETE
	@Path("trainerlevels/{trlv_pk}")
	public boolean deleteTrainerlevel(@PathParam("trlv_pk") final Integer trlv_pk) {
		return this.ctx.delete(TRAINERLEVELS).where(TRAINERLEVELS.TRLV_PK.eq(trlv_pk)).execute() > 0;
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
				return EMPLOYEES_ROLES.EMRO_TRLV_FK;
			default:
				return null;
		}
	}

	private static void insertTypes(final Integer trlv_pk, final List<Integer> types, final DSLContext transactionCtx) {
		if (types.isEmpty()) {
			transactionCtx.insertInto(TRAINERLEVELS_TRAININGTYPES, TRAINERLEVELS_TRAININGTYPES.TLTR_TRLV_FK, TRAINERLEVELS_TRAININGTYPES.TLTR_TRTY_FK)
					.select(DSL.select(DSL.val(trlv_pk), DSL.field("trlv_trty_fk", Integer.class))
							.from(DSL.values(types.stream().map(DSL::row).toArray(Row1[]::new)).as("unused", "trlv_trty_fk")))
					.execute();
		}
	}
}
