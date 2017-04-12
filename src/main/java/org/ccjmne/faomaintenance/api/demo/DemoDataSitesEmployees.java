package org.ccjmne.faomaintenance.api.demo;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.DEPARTMENTS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.UPDATES;

import java.sql.Date;
import java.time.LocalDate;
import java.time.Month;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.ccjmne.faomaintenance.api.utils.Constants;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.DepartmentsRecord;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.EmployeesRecord;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.SitesRecord;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Insert;
import org.jooq.InsertValuesStep2;
import org.jooq.InsertValuesStep4;
import org.jooq.InsertValuesStep7;
import org.jooq.Table;
import org.jooq.impl.DSL;

import io.codearte.jfairy.Fairy;
import io.codearte.jfairy.producer.person.Person;

public class DemoDataSitesEmployees {

	public static final Fairy FAIRY = Fairy.create(Locale.FRENCH);

	public static void generate(final DSLContext ctx) {
		addDepartments(ctx.insertInto(DEPARTMENTS, DEPARTMENTS.DEPT_ID, DEPARTMENTS.DEPT_NAME), 10, "DEPT%02d").execute();
		addSites(ctx.insertInto(SITES, SITES.SITE_PK, SITES.SITE_NAME, SITES.SITE_ADDRESS, SITES.SITE_DEPT_FK), 200, "SITE%03d").execute();
		for (int i = 0; i < 10; i++) {
			addEmployees(
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

		// Update dated from NCLSDevelopment's birth day :)
		final Integer update = ctx.insertInto(UPDATES, UPDATES.UPDT_DATE).values(Date.valueOf(LocalDate.of(2014, Month.DECEMBER, 8))).returning(UPDATES.UPDT_PK)
				.fetchOne()
				.get(UPDATES.UPDT_PK);

		ctx.insertInto(SITES_EMPLOYEES, SITES_EMPLOYEES.SIEM_UPDT_FK, SITES_EMPLOYEES.SIEM_SITE_FK, SITES_EMPLOYEES.SIEM_EMPL_FK)
				.select(DSL.select(DSL.val(update), DSL.field("site_pk", String.class), DSL.field("empl_pk", String.class))
						.from(
								DSL.select(
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

	@SuppressWarnings("unchecked")
	private static Insert<?> addEmployees(final Insert<?> query, final int i, final String pk) {
		final Person person = FAIRY.person();
		return ((InsertValuesStep7<EmployeesRecord, String, String, String, Date, String, Boolean, Boolean>) (i == 1 ? query : addEmployees(query, i - 1, pk)))
				.values(
						String.format(pk, Integer.valueOf(i)),
						person.firstName(),
						person.lastName().toUpperCase(),
						new Date(person.dateOfBirth().toDate().getTime()),
						person.companyEmail().replaceFirst("@.*$", "@orca-demo.com"),
						Boolean.valueOf(person.isMale()),
						Boolean.valueOf(FAIRY.baseProducer().trueOrFalse() || FAIRY.baseProducer().trueOrFalse()));
	}

	@SuppressWarnings("unchecked")
	private static Insert<?> addSites(final Insert<?> query, final int i, final String pk) {
		final String city = FAIRY.person().getAddress().getCity();
		return ((InsertValuesStep4<SitesRecord, String, String, String, Integer>) (i == 1 ? query : addSites(query, i - 1, pk)))
				.values(asFields(
									String.format(pk, Integer.valueOf(i)),
									city,
									city.replaceAll("[\\s']", "").toLowerCase() + "@orca-demo.com",
									random(DEPARTMENTS, DEPARTMENTS.DEPT_PK, DEPARTMENTS.DEPT_PK.ne(Constants.UNASSIGNED_DEPARTMENT))));
	}

	@SuppressWarnings("unchecked")
	private static Insert<?> addDepartments(final Insert<?> query, final int i, final String pk) {
		return ((InsertValuesStep2<DepartmentsRecord, String, String>) (i == 1 ? query : addDepartments(query, i - 1, pk)))
				.values(String.format(pk, Integer.valueOf(i)), String.format("Département %c", Integer.valueOf(('A' - 1) + i)));
	}

	private static <R> Field<R> random(final Table<?> table, final Field<R> field, final Condition... conditions) {
		return DSL.select(field).from(table).where(conditions).orderBy(DSL.rand()).limit(1).asField();
	}

	private static List<? extends Field<?>> asFields(final Object... values) {
		return Arrays.asList(values).stream().map(v -> v instanceof Field<?> ? (Field<?>) v : DSL.val(v)).collect(Collectors.toList());
	}
}
