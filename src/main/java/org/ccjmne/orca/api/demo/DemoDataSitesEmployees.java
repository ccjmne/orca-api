package org.ccjmne.orca.api.demo;

import static org.ccjmne.orca.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES_TAGS;
import static org.ccjmne.orca.jooq.classes.Tables.TAGS;
import static org.ccjmne.orca.jooq.classes.Tables.UPDATES;

import java.sql.Date;
import java.time.LocalDate;
import java.time.Month;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.jooq.classes.tables.records.EmployeesRecord;
import org.ccjmne.orca.jooq.classes.tables.records.SitesRecord;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.InsertValuesStep3;
import org.jooq.InsertValuesStep7;
import org.jooq.Record1;
import org.jooq.Row1;
import org.jooq.Table;
import org.jooq.impl.DSL;

import io.codearte.jfairy.Fairy;
import io.codearte.jfairy.producer.person.Person;

public class DemoDataSitesEmployees {

	public static final Fairy FAIRY = Fairy.create(Locale.FRENCH);

	@SuppressWarnings("null")
	public static void generate(final DSLContext ctx) {
		DemoDataSitesEmployees.addSites(ctx.insertInto(SITES, SITES.SITE_PK, SITES.SITE_NAME, SITES.SITE_ADDRESS), 200, "SITE%03d").execute();

		// GEO tags
		final Integer geoTag = ctx.insertInto(TAGS, TAGS.TAGS_NAME, TAGS.TAGS_SHORT, TAGS.TAGS_TYPE, TAGS.TAGS_HEX_COLOUR)
				.values("Situation Géographique", "GEO", Constants.TAGS_TYPE_STRING, "#C71585").returning(TAGS.TAGS_PK).fetchOne().get(TAGS.TAGS_PK);
		DemoDataSitesEmployees.insertTags(ctx, geoTag, "ABCDEF".chars()
				.mapToObj(c -> String.format("Zone %s", Character.valueOf((char) c)))
				.map(DSL::row)
				.toArray(Row1[]::new));

		// TYPE tags
		final Integer typeTag = ctx.insertInto(TAGS, TAGS.TAGS_NAME, TAGS.TAGS_SHORT, TAGS.TAGS_TYPE, TAGS.TAGS_HEX_COLOUR)
				.values("Type d'Activité", "TYPE", Constants.TAGS_TYPE_STRING, "#795548").returning(TAGS.TAGS_PK).fetchOne().get(TAGS.TAGS_PK);
		DemoDataSitesEmployees.insertTags(ctx, typeTag, Arrays.asList("Usine", "Bureaux", "Entrepôt", "Boutique").stream()
				.map(DSL::row)
				.toArray(Row1[]::new));

		// ERP tags
		final Integer erpTag = ctx.insertInto(TAGS, TAGS.TAGS_NAME, TAGS.TAGS_SHORT, TAGS.TAGS_TYPE, TAGS.TAGS_HEX_COLOUR)
				.values("Établissement Recevant du Public", "ERP", Constants.TAGS_TYPE_BOOLEAN, "#009688").returning(TAGS.TAGS_PK).fetchOne().get(TAGS.TAGS_PK);
		ctx.insertInto(SITES_TAGS, SITES_TAGS.SITA_SITE_FK, SITES_TAGS.SITA_TAGS_FK, SITES_TAGS.SITA_VALUE)
				.select(DSL
						.select(SITES.SITE_PK,
								DSL.val(erpTag),
								DSL.coerce(DSL.field(DSL.cast(DSL.rand(), Integer.class).mod(Integer.valueOf(2)).eq(Integer.valueOf(0))), String.class))
						.from(SITES)
						.where(SITES.SITE_PK.ne(Constants.UNASSIGNED_SITE)))
				.execute();

		for (int i = 0; i < 10; i++) {
			DemoDataSitesEmployees.addEmployees(
												ctx.insertInto(
																EMPLOYEES,
																EMPLOYEES.EMPL_PK,
																EMPLOYEES.EMPL_FIRSTNAME,
																EMPLOYEES.EMPL_SURNAME,
																EMPLOYEES.EMPL_DOB,
																EMPLOYEES.EMPL_ADDRESS,
																EMPLOYEES.EMPL_GENDER,
																EMPLOYEES.EMPL_PERMANENT),
												500,
												String.format("EMPL%02d%%03d", Integer.valueOf(i)))
					.execute();
		}

		// Update dated from NCLS Development's birth day :)
		final Integer update = ctx.insertInto(UPDATES, UPDATES.UPDT_DATE).values(Date.valueOf(LocalDate.of(2014, Month.DECEMBER, 8))).returning(UPDATES.UPDT_PK)
				.fetchOne()
				.get(UPDATES.UPDT_PK);

		ctx.insertInto(SITES_EMPLOYEES, SITES_EMPLOYEES.SIEM_UPDT_FK, SITES_EMPLOYEES.SIEM_SITE_FK, SITES_EMPLOYEES.SIEM_EMPL_FK)
				.select(DSL.select(DSL.val(update), DSL.field("site_pk", String.class), DSL.field("empl_pk", String.class))
						.from(DSL.select(
											EMPLOYEES.EMPL_PK,
											DSL.floor(DSL.rand().mul(DSL.select(DSL.count()).from(SITES).where(SITES.SITE_PK.ne(Constants.UNASSIGNED_SITE))
													.asField()).add(DSL.val(1)))
													.as("linked_site_id"))
								.from(EMPLOYEES).where(EMPLOYEES.EMPL_PK.ne(Constants.USER_ROOT)).asTable("employees2"))
						.join(DSL.select(
											SITES.SITE_PK,
											DSL.rowNumber().over().orderBy(DSL.rand()).as("site_id"))
								.from(SITES).where(SITES.SITE_PK.ne(Constants.UNASSIGNED_SITE)).asTable("sites2"))
						.on(DSL.field("linked_site_id").eq(DSL.field("site_id"))))
				.execute();
	}

	public static void insertTags(final DSLContext ctx, final Integer tags_pk, final Row1<String>[] values) {
		final Table<Record1<String>> tagsTable = DSL.values(values).asTable();
		final Field<String> tagField = tagsTable.field(0, String.class);
		ctx.insertInto(SITES_TAGS, SITES_TAGS.SITA_SITE_FK, SITES_TAGS.SITA_TAGS_FK, SITES_TAGS.SITA_VALUE)
				.select(DSL
						.select(DSL.field("site", String.class), DSL.val(tags_pk), DSL.field("tag", String.class))
						.from(DSL.select(
											SITES.SITE_PK.as("site"),
											DSL
													.rowNumber().over().orderBy(DSL.rand())
													.mod(Integer.valueOf(values.length))
													.plus(Integer.valueOf(1))
													.as("tag_fk"))
								.from(SITES)
								.where(SITES.SITE_PK.ne(Constants.UNASSIGNED_SITE)))
						.join(DSL.select(tagField.as("tag"), DSL.rowNumber().over().as("tag_pk")).from(tagsTable))
						.on(DSL.field("tag_fk").eq(DSL.field("tag_pk"))))
				.execute();
	}

	// TODO: rewrite bulk insert of employees and sites
	private static InsertValuesStep7<EmployeesRecord, String, String, String, Date, String, Boolean, Boolean> addEmployees(
																															final InsertValuesStep7<EmployeesRecord, String, String, String, Date, String, Boolean, Boolean> query,
																															final int i,
																															final String pk) {
		final Person person = FAIRY.person();
		return (i == 1 ? query : DemoDataSitesEmployees.addEmployees(query, i - 1, pk))
				.values(
						String.format(pk, Integer.valueOf(i)),
						person.getFirstName(),
						person.getLastName().toUpperCase(),
						new Date(person.getDateOfBirth().toDate().getTime()),
						person.getCompanyEmail().replaceFirst("@.*$", "@orca-solution.com"),
						Boolean.valueOf(person.isMale()),
						Boolean.valueOf(FAIRY.baseProducer().trueOrFalse() || FAIRY.baseProducer().trueOrFalse()));
	}

	// TODO: rewrite bulk insert of employees and sites
	private static InsertValuesStep3<SitesRecord, String, String, String> addSites(
																					final InsertValuesStep3<SitesRecord, String, String, String> query,
																					final int i,
																					final String pk) {
		final String city = FAIRY.person().getAddress().getCity();
		return (i == 1 ? query : DemoDataSitesEmployees.addSites(query, i - 1, pk))
				.values(DemoDataSitesEmployees.asFields(
														String.format(pk, Integer.valueOf(i)),
														city,
														StringUtils.stripAccents(city.replaceAll("[\\s']", "").toLowerCase()) + "@orca-solution.com"));
	}

	private static List<? extends Field<?>> asFields(final Object... values) {
		return Arrays.asList(values).stream().map(v -> v instanceof Field<?> ? (Field<?>) v : DSL.val(v)).collect(Collectors.toList());
	}
}
