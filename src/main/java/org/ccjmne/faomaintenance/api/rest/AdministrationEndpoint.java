package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES_ROLES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.ROLES;

import java.sql.Date;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;

import org.ccjmne.faomaintenance.jooq.classes.tables.records.EmployeesRolesRecord;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.RolesRecord;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.InsertValuesStep2;
import org.jooq.Record10;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.Support;
import org.jooq.impl.DSL;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

@Path("admin")
public class AdministrationEndpoint {

	private final DSLContext ctx;
	private final ResourcesEndpoint resources;
	private final Supplier<Result<RolesRecord>> roles;

	private static final int GENERATED_PASSWORD_LENGTH = 8;
	private static final String GENERATED_PASSWORD_CHARS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
	private static final String BASIC_USER_ROLENAME = "user";
	private static final Random RANDOM = new Random();

	@Support({ SQLDialect.POSTGRES })
	private static <T> Field<T[]> arrayAgg(final Field<T> field) {
		return DSL.field("array_agg({0})", field.getDataType().getArrayDataType(), field);
	}

	@Inject
	public AdministrationEndpoint(final DSLContext ctx, final ResourcesEndpoint resources) {
		this.ctx = ctx;
		this.resources = resources;
		this.roles = Suppliers.memoizeWithExpiration(() -> this.ctx.selectFrom(ROLES).fetch(), 1, TimeUnit.DAYS);
	}

	@GET
	@Path("roles")
	public Result<RolesRecord> getAvailableRoles() {
		return this.roles.get();
	}

	@GET
	@Path("users")
	public Result<Record10<String, String, String, Boolean, Date, Boolean, String, Date, String, String[]>> getUsers() {
		return this.ctx
				.select(
						EMPLOYEES.EMPL_PK,
						EMPLOYEES.EMPL_FIRSTNAME,
						EMPLOYEES.EMPL_SURNAME,
						EMPLOYEES.EMPL_PERMANENT,
						EMPLOYEES.EMPL_DOB,
						EMPLOYEES.EMPL_GENDER,
						EMPLOYEES.EMPL_NOTES,
						EMPLOYEES.EMPL_SST_OPTOUT,
						EMPLOYEES.EMPL_ADDR,
						arrayAgg(EMPLOYEES_ROLES.EMRO_ROLE_FK).as("roles"))
				.from(EMPLOYEES).join(EMPLOYEES_ROLES).on(EMPLOYEES_ROLES.EMPL_PK.eq(EMPLOYEES.EMPL_PK))
				.where(EMPLOYEES.EMPL_PK.ne("admin"))
				.groupBy(
							EMPLOYEES.EMPL_PK,
							EMPLOYEES.EMPL_FIRSTNAME,
							EMPLOYEES.EMPL_SURNAME,
							EMPLOYEES.EMPL_PERMANENT,
							EMPLOYEES.EMPL_DOB,
							EMPLOYEES.EMPL_GENDER,
							EMPLOYEES.EMPL_NOTES,
							EMPLOYEES.EMPL_SST_OPTOUT,
							EMPLOYEES.EMPL_ADDR)
				.fetch();
	}

	@GET
	@Path("users/{empl_pk}")
	public Map<String, Object> getUserInfo(@PathParam("empl_pk") final String empl_pk) {
		final Map<String, Object> res = this.resources.lookupEmployee(empl_pk).intoMap();
		res.put("roles", this.ctx.select().from(EMPLOYEES_ROLES).where(EMPLOYEES_ROLES.EMPL_PK.eq(empl_pk)).fetch(EMPLOYEES_ROLES.EMRO_ROLE_FK));
		return res;
	}

	@PUT
	@Path("users/{empl_pk}/roles")
	@Consumes(MediaType.APPLICATION_JSON)
	public String setRoles(@PathParam("empl_pk") final String empl_pk, final Set<String> roles) {
		if ((roles == null) || roles.isEmpty()) {
			this.ctx.delete(EMPLOYEES_ROLES).where(EMPLOYEES_ROLES.EMPL_PK.eq(empl_pk)).execute();
			return null;
		}

		if (!this.roles.get().getValues(ROLES.ROLE_NAME).containsAll(roles)) {
			throw new IllegalArgumentException("Invalid role(s) specified.");
		}

		if (!roles.contains(BASIC_USER_ROLENAME)) {
			throw new IllegalArgumentException(String.format("Advanced privileges require the '%s' role.", BASIC_USER_ROLENAME));
		}

		this.ctx.delete(EMPLOYEES_ROLES).where(EMPLOYEES_ROLES.EMPL_PK.eq(empl_pk)).execute();
		final InsertValuesStep2<EmployeesRolesRecord, String, String> query = this.ctx.insertInto(
																									EMPLOYEES_ROLES,
																									EMPLOYEES_ROLES.EMPL_PK,
																									EMPLOYEES_ROLES.EMRO_ROLE_FK);
		roles.forEach(role -> query.values(empl_pk, role));
		query.execute();

		final String password = this.ctx.selectFrom(EMPLOYEES).where(EMPLOYEES.EMPL_PK.eq(empl_pk)).fetchOne(EMPLOYEES.EMPL_PWD);
		if ((password == null) || password.isEmpty()) {
			return reinitialisePassword(empl_pk);
		}

		return null;
	}

	@DELETE
	@Path("users/{empl_pk}/password")
	public String reinitialisePassword(@PathParam("empl_pk") final String empl_pk) {
		final StringBuilder builder = new StringBuilder(GENERATED_PASSWORD_LENGTH);
		for (int i = 0; i < GENERATED_PASSWORD_LENGTH; i++) {
			builder.append(GENERATED_PASSWORD_CHARS.charAt(RANDOM.nextInt(GENERATED_PASSWORD_CHARS.length())));
		}

		final String password = builder.toString();
		this.ctx.update(EMPLOYEES).set(EMPLOYEES.EMPL_PWD, password).where(EMPLOYEES.EMPL_PK.eq(empl_pk)).execute();
		return password;
	}
}
