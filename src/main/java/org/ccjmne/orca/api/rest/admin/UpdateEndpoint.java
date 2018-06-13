package org.ccjmne.orca.api.rest.admin;

import static org.ccjmne.orca.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.UPDATES;
import static org.ccjmne.orca.jooq.classes.Tables.USERS;

import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

import org.ccjmne.orca.api.modules.Restrictions;
import org.ccjmne.orca.api.rest.fetch.ResourcesEndpoint;
import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.api.utils.SafeDateFormat;
import org.ccjmne.orca.api.utils.Transactions;
import org.ccjmne.orca.jooq.classes.Sequences;
import org.ccjmne.orca.jooq.classes.tables.records.SitesEmployeesRecord;
import org.jooq.DSLContext;
import org.jooq.InsertValuesStep3;
import org.jooq.TableField;
import org.jooq.impl.DSL;

@Path("update")
public class UpdateEndpoint {

	private static final Pattern GENDER_REGEX = Pattern.compile("^\\s*m", Pattern.CASE_INSENSITIVE);
	private static final Pattern FIRST_LETTER = Pattern.compile("\\b(\\w)", Pattern.UNICODE_CHARACTER_CLASS);

	private final DSLContext ctx;

	@Inject
	public UpdateEndpoint(final DSLContext ctx, final ResourcesEndpoint resources, final Restrictions restrictions) {
		if (!restrictions.canManageSitesAndTags()) {
			throw new ForbiddenException();
		}

		this.ctx = ctx;
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	public void process(final List<Map<String, Object>> employees) {
		Transactions.with(this.ctx, transactionCtx -> {
			final Integer updt_pk = new Integer(transactionCtx.nextval(Sequences.UPDATES_UPDT_PK_SEQ).intValue());

			// No more than ONE update per day
			transactionCtx.delete(UPDATES).where(UPDATES.UPDT_DATE.eq(DSL.currentDate())).execute();
			transactionCtx.insertInto(UPDATES).set(UPDATES.UPDT_PK, updt_pk).set(UPDATES.UPDT_DATE, DSL.currentDate()).execute();

			// TODO: rewrite using a TableLike (basically a Row[])
			// ... and w/o try-catch
			try (final InsertValuesStep3<SitesEmployeesRecord, Integer, Integer, Integer> query = transactionCtx
					.insertInto(SITES_EMPLOYEES, SITES_EMPLOYEES.SIEM_UPDT_FK, SITES_EMPLOYEES.SIEM_SITE_FK, SITES_EMPLOYEES.SIEM_EMPL_FK)) {
				for (final Map<String, Object> employee : employees) {
					query.values(	updt_pk, (Integer) employee.get(SITES_EMPLOYEES.SIEM_SITE_FK.getName()),
									UpdateEndpoint.updateEmployee(employee, transactionCtx));
				}

				query.execute();
			} catch (final ParseException e) {
				throw new RuntimeException(e);
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
													DSL.val(Constants.DECOMMISSIONED_SITE),
													DSL.val(updt_pk))
									.from(EMPLOYEES)
									.where(EMPLOYEES.EMPL_PK
											.notIn(transactionCtx.select(SITES_EMPLOYEES.SIEM_EMPL_FK).from(SITES_EMPLOYEES)
													.where(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(updt_pk)))))
					.execute();
		});
	}

	private static String titleCase(final String str) {
		final StringBuilder res = new StringBuilder(str.toLowerCase());
		final Matcher matcher = FIRST_LETTER.matcher(str);
		while (matcher.find()) {
			res.replace(matcher.start(), matcher.end(), matcher.group().toUpperCase());
		}

		return res.toString();
	}

	private static Integer updateEmployee(final Map<String, Object> employee, final DSLContext context) throws ParseException {
		final Integer empl_pk = (Integer) employee.get(EMPLOYEES.EMPL_PK.getName());
		final Map<TableField<?, ?>, Object> record = new HashMap<>();
		record.put(EMPLOYEES.EMPL_FIRSTNAME, UpdateEndpoint.titleCase((String) employee.get(EMPLOYEES.EMPL_FIRSTNAME.getName())));
		record.put(EMPLOYEES.EMPL_SURNAME, ((String) employee.get(EMPLOYEES.EMPL_SURNAME.getName())).toUpperCase());
		record.put(EMPLOYEES.EMPL_DOB, SafeDateFormat.parseAsSql((String) employee.get(EMPLOYEES.EMPL_DOB.getName())));
		record.put(EMPLOYEES.EMPL_PERMANENT, Boolean.valueOf("CDI".equalsIgnoreCase((String) employee.get(EMPLOYEES.EMPL_PERMANENT.getName()))));
		record.put(EMPLOYEES.EMPL_GENDER, Boolean.valueOf(GENDER_REGEX.matcher((String) employee.get(EMPLOYEES.EMPL_GENDER.getName())).find(0)));
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
