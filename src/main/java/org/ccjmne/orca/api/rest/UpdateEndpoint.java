package org.ccjmne.orca.api.rest;

import static org.ccjmne.orca.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES_TAGS;
import static org.ccjmne.orca.jooq.classes.Tables.UPDATES;
import static org.ccjmne.orca.jooq.classes.Tables.USERS;

import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.ObjectUtils;
import org.ccjmne.orca.api.modules.Restrictions;
import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.api.utils.SafeDateFormat;
import org.ccjmne.orca.api.utils.Transactions;
import org.ccjmne.orca.jooq.classes.Sequences;
import org.ccjmne.orca.jooq.classes.tables.records.SitesEmployeesRecord;
import org.ccjmne.orca.jooq.classes.tables.records.SitesTagsRecord;
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

	@PUT
	@Path("sites/{site_pk}")
	@Consumes(MediaType.APPLICATION_JSON)
	@SuppressWarnings("unchecked")
	public Boolean insertSite(@PathParam("site_pk") final Integer site_pk, final Map<String, Object> siteDefinition) {
		return Transactions.with(this.ctx, transactionCtx -> {
			final Map<String, Object> tags = ObjectUtils.defaultIfNull((Map<String, Object>) siteDefinition.remove("tags"), Collections.emptyMap());
			final boolean exists = transactionCtx.fetchExists(SITES, SITES.SITE_PK.eq(site_pk));
			if (exists) {
				transactionCtx.update(SITES).set(siteDefinition).where(SITES.SITE_PK.eq(site_pk)).execute();
			} else {
				transactionCtx.insertInto(SITES)
						.set(siteDefinition)
						.set(SITES.SITE_PK, site_pk)
						.execute();
			}

			transactionCtx.deleteFrom(SITES_TAGS).where(SITES_TAGS.SITA_SITE_FK.eq(site_pk)).execute();
			transactionCtx.batchInsert(tags.entrySet().stream()
					.map(tag -> {
						if (Constants.TAGS_VALUE_NONE.equals(tag.getValue()) || Constants.TAGS_VALUE_UNIVERSAL.equals(tag.getValue())) {
							throw new IllegalArgumentException(String.format("Invalid tag value: '%s'", tag.getValue()));
						}

						return new SitesTagsRecord(site_pk, Integer.valueOf(tag.getKey()), String.valueOf(tag.getValue()));
					}).collect(Collectors.toList())).execute();
			return Boolean.valueOf(!exists);
		});
	}

	/**
	 * @return <code>true</code> iff that site was actually existing
	 */
	@DELETE
	@Path("sites/{site_pk}")
	public Boolean deleteSite(@PathParam("site_pk") final Integer site_pk) {
		// Database CASCADEs the deletion of linked users, if any
		// Database CASCADEs the deletion of its tags
		return Transactions.with(this.ctx, transactionCtx -> {
			// Set deleted entity employees' site to UNASSIGNED
			final boolean exists = transactionCtx.selectFrom(SITES).where(SITES.SITE_PK.equal(site_pk)).fetch().isNotEmpty();
			if (exists) {
				transactionCtx.update(SITES_EMPLOYEES)
						.set(SITES_EMPLOYEES.SIEM_SITE_FK, Constants.DECOMMISSIONED_SITE)
						.where(SITES_EMPLOYEES.SIEM_SITE_FK.eq(site_pk)).execute();
				transactionCtx.delete(SITES).where(SITES.SITE_PK.eq(site_pk)).execute();
			}

			return Boolean.valueOf(exists);
		});
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
