package org.ccjmne.orca.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.DEPARTMENTS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.UPDATES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.USERS;

import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.ccjmne.faomaintenance.jooq.classes.Sequences;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.SitesEmployeesRecord;
import org.ccjmne.orca.api.modules.Restrictions;
import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.api.utils.SafeDateFormat;
import org.jooq.DSLContext;
import org.jooq.InsertValuesStep3;
import org.jooq.TableField;
import org.jooq.impl.DSL;

@Path("update")
public class UpdateEndpoint {

	private static final Pattern GENDER_REGEX = Pattern.compile("^\\s*m", Pattern.CASE_INSENSITIVE);
	private static final Pattern FIRST_LETTER = Pattern.compile("\\b(\\w)");

	private final DSLContext ctx;

	@Inject
	public UpdateEndpoint(final DSLContext ctx, final ResourcesEndpoint resources, final Restrictions restrictions) {
		if (!restrictions.canManageSites()) {
			throw new ForbiddenException();
		}

		this.ctx = ctx;
	}

	@POST
	@Path("departments")
	@Consumes(MediaType.APPLICATION_JSON)
	public Integer createDept(final Map<String, String> dept) {
		final Integer dept_pk = new Integer(this.ctx.nextval(Sequences.DEPARTMENTS_DEPT_PK_SEQ).intValue());
		updateDepartment(dept_pk, dept);
		return dept_pk;
	}

	@PUT
	@Path("departments/{dept_pk}")
	@Consumes(MediaType.APPLICATION_JSON)
	public boolean updateDepartment(@PathParam("dept_pk") final Integer dept_pk, final Map<String, String> dept) {
		if (this.ctx.fetchExists(DEPARTMENTS, DEPARTMENTS.DEPT_PK.eq(dept_pk))) {
			this.ctx.update(DEPARTMENTS)
					.set(DEPARTMENTS.DEPT_NAME, dept.get(DEPARTMENTS.DEPT_NAME.getName()))
					.set(DEPARTMENTS.DEPT_ID, dept.get(DEPARTMENTS.DEPT_ID.getName()))
					.set(DEPARTMENTS.DEPT_NOTES, dept.get(DEPARTMENTS.DEPT_NOTES.getName()))
					.where(DEPARTMENTS.DEPT_PK.eq(dept_pk)).execute();
			return false;
		}

		this.ctx.insertInto(DEPARTMENTS, DEPARTMENTS.DEPT_PK, DEPARTMENTS.DEPT_NAME, DEPARTMENTS.DEPT_ID, DEPARTMENTS.DEPT_NOTES)
				.values(dept_pk, dept.get(DEPARTMENTS.DEPT_NAME.getName()), dept.get(DEPARTMENTS.DEPT_ID.getName()), dept.get(DEPARTMENTS.DEPT_NOTES.getName()))
				.execute();
		return true;
	}

	@PUT
	@Path("sites/{site_pk}")
	@Consumes(MediaType.APPLICATION_JSON)
	public boolean updateSite(@PathParam("site_pk") final String site_pk, final Map<String, String> site) {
		if (this.ctx.fetchExists(SITES, SITES.SITE_PK.eq(site_pk))) {
			this.ctx.update(SITES)
					.set(SITES.SITE_PK, site.getOrDefault(SITES.SITE_PK.getName(), site_pk))
					.set(SITES.SITE_NAME, site.get(SITES.SITE_NAME.getName()))
					.set(SITES.SITE_DEPT_FK, Integer.valueOf(site.get(SITES.SITE_DEPT_FK.getName())))
					.set(SITES.SITE_NOTES, site.get(SITES.SITE_NOTES.getName()))
					.set(SITES.SITE_ADDRESS, site.get(SITES.SITE_ADDRESS.getName()))
					.where(SITES.SITE_PK.eq(site_pk)).execute();
			return false;
		}

		this.ctx.insertInto(SITES, SITES.SITE_PK, SITES.SITE_NAME, SITES.SITE_DEPT_FK, SITES.SITE_NOTES, SITES.SITE_ADDRESS)
				.values(
						site_pk,
						site.get(SITES.SITE_NAME.getName()),
						Integer.valueOf(site.get(SITES.SITE_DEPT_FK.getName())),
						site.get(SITES.SITE_NOTES.getName()),
						site.get(SITES.SITE_ADDRESS.getName()))
				.execute();
		return true;
	}

	@DELETE
	@Path("departments/{dept_pk}")
	public boolean deleteDept(@PathParam("dept_pk") final Integer dept_pk) {
		// Database CASCADEs the deletion of linked users, if any
		return this.ctx.delete(DEPARTMENTS).where(DEPARTMENTS.DEPT_PK.eq(dept_pk)).execute() == 1;
	}

	@DELETE
	@Path("sites/{site_pk}")
	public boolean deleteSite(@PathParam("site_pk") final String site_pk) {
		// Database CASCADEs the deletion of linked users, if any
		final boolean exists = this.ctx.selectFrom(SITES).where(SITES.SITE_PK.equal(site_pk)).fetch().isNotEmpty();
		if (exists) {
			this.ctx.delete(SITES).where(SITES.SITE_PK.eq(site_pk)).execute();
			this.ctx.delete(SITES_EMPLOYEES).where(SITES_EMPLOYEES.SIEM_SITE_FK.eq(site_pk)).execute();
		}

		return exists;
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	public Response process(final List<Map<String, String>> employees) {
		try {
			this.ctx.transaction(config -> {
				try (final DSLContext transactionCtx = DSL.using(config)) {
					final Integer updt_pk = new Integer(transactionCtx.nextval(Sequences.UPDATES_UPDT_PK_SEQ).intValue());

					// No more than ONE update per day
					transactionCtx.delete(UPDATES).where(UPDATES.UPDT_DATE.eq(DSL.currentDate())).execute();
					transactionCtx.insertInto(UPDATES).set(UPDATES.UPDT_PK, updt_pk).set(UPDATES.UPDT_DATE, DSL.currentDate()).execute();

					try (final InsertValuesStep3<SitesEmployeesRecord, Integer, String, String> query = transactionCtx
							.insertInto(SITES_EMPLOYEES, SITES_EMPLOYEES.SIEM_UPDT_FK, SITES_EMPLOYEES.SIEM_SITE_FK, SITES_EMPLOYEES.SIEM_EMPL_FK)) {
						for (final Map<String, String> employee : employees) {
							query.values(updt_pk, employee.get(SITES_EMPLOYEES.SIEM_SITE_FK.getName()), updateEmployee(employee, transactionCtx));
						}

						query.execute();
					}

					// Remove all privileges of the unassigned employees
					transactionCtx
							.delete(USERS)
							.where(USERS.USER_TYPE.eq(Constants.USERTYPE_EMPLOYEE).and(USERS.USER_EMPL_FK
									.notIn(DSL.select(SITES_EMPLOYEES.SIEM_EMPL_FK).from(SITES_EMPLOYEES).where(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(updt_pk)))))
							.and(USERS.USER_ID.ne(Constants.USER_ROOT))
							.execute();

					// ... and set their site to #0 ('unassigned')
					transactionCtx.insertInto(SITES_EMPLOYEES, SITES_EMPLOYEES.SIEM_EMPL_FK, SITES_EMPLOYEES.SIEM_SITE_FK, SITES_EMPLOYEES.SIEM_UPDT_FK)
							.select(
									transactionCtx.select(
															EMPLOYEES.EMPL_PK,
															DSL.val(Constants.UNASSIGNED_SITE),
															DSL.val(updt_pk))
											.from(EMPLOYEES)
											.where(EMPLOYEES.EMPL_PK
													.notIn(transactionCtx.select(SITES_EMPLOYEES.SIEM_EMPL_FK).from(SITES_EMPLOYEES)
															.where(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(updt_pk)))))
							.execute();
				}
			});

			return Response.ok().build();
		} catch (final Exception e) {
			return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build();
		}
	}

	private static String capitalise(final String str) {
		final StringBuilder res = new StringBuilder(str.toLowerCase());
		final Matcher matcher = FIRST_LETTER.matcher(res);
		while (matcher.find()) {
			res.replace(matcher.start(), matcher.start() + 1, matcher.group().toUpperCase());
		}

		return res.toString();
	}

	private static String updateEmployee(final Map<String, String> employee, final DSLContext context) throws ParseException {
		final String empl_pk = employee.get(EMPLOYEES.EMPL_PK.getName());
		final Map<TableField<?, ?>, Object> record = new HashMap<>();
		record.put(EMPLOYEES.EMPL_FIRSTNAME, capitalise(employee.get(EMPLOYEES.EMPL_FIRSTNAME.getName())));
		record.put(EMPLOYEES.EMPL_SURNAME, employee.get(EMPLOYEES.EMPL_SURNAME.getName()).toUpperCase());
		record.put(EMPLOYEES.EMPL_DOB, SafeDateFormat.parseAsSql(employee.get(EMPLOYEES.EMPL_DOB.getName())));
		record.put(EMPLOYEES.EMPL_PERMANENT, Boolean.valueOf("CDI".equalsIgnoreCase(employee.get(EMPLOYEES.EMPL_PERMANENT.getName()))));
		record.put(EMPLOYEES.EMPL_GENDER, Boolean.valueOf(GENDER_REGEX.matcher(employee.get(EMPLOYEES.EMPL_GENDER.getName())).find(0)));
		record.put(EMPLOYEES.EMPL_ADDRESS, employee.get(EMPLOYEES.EMPL_ADDRESS.getName()));

		if (context.fetchExists(EMPLOYEES, EMPLOYEES.EMPL_PK.eq(empl_pk))) {
			context.update(EMPLOYEES).set(record).where(EMPLOYEES.EMPL_PK.eq(empl_pk)).execute();
		} else {
			record.put(EMPLOYEES.EMPL_PK, empl_pk);
			context.insertInto(EMPLOYEES).set(record).execute();
		}

		return empl_pk;
	}
}
