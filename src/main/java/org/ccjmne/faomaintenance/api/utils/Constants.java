package org.ccjmne.faomaintenance.api.utils;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.CERTIFICATES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES_CERTIFICATES_OPTOUT;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS_TRAINERS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGTYPES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGTYPES_CERTIFICATES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.UPDATES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.USERS;

import java.sql.Date;

import org.ccjmne.faomaintenance.jooq.classes.tables.Updates;
import org.jooq.Condition;
import org.jooq.DatePart;
import org.jooq.Field;
import org.jooq.Record4;
import org.jooq.Record5;
import org.jooq.Record7;
import org.jooq.Select;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.types.YearToMonth;

public class Constants {

	// ---- API CONSTANTS
	public static final String FIELDS_ALL = "all";
	private static final Integer NO_UPDATE = Integer.valueOf(-1);

	public static final String STATUS_SUCCESS = "success";
	public static final String STATUS_WARNING = "warning";
	public static final String STATUS_DANGER = "danger";
	// ----

	// ---- DATABASE CONSTANTS
	public static final String TRNG_OUTCOME_COMPLETED = "COMPLETED";
	public static final String TRNG_OUTCOME_SCHEDULED = "SCHEDULED";
	public static final String EMPL_OUTCOME_VALIDATED = "VALIDATED";
	public static final String EMPL_OUTCOME_PENDING = "PENDING";
	public static final String EMPL_OUTCOME_FLUNKED = "FLUNKED";

	public static final String USER_ROOT = "root";

	public static final String UNASSIGNED_SITE = "0";
	public static final Integer UNASSIGNED_DEPT = Integer.valueOf(0);
	public static final Integer UNASSIGNED_TRAINERPROFILE = Integer.valueOf(0);

	public static final String ROLE_USER = "user";
	public static final String ROLE_ACCESS = "access";
	public static final String ROLE_TRAINER = "trainer";
	public static final String ROLE_ADMIN = "admin";

	public static final String USERTYPE_EMPLOYEE = "employee";
	public static final String USERTYPE_SITE = "site";
	public static final String USERTYPE_DEPARTMENT = "department";

	public static final Integer ACCESS_LEVEL_TRAININGS = Integer.valueOf(4);
	public static final Integer ACCESS_LEVEL_ALL_SITES = Integer.valueOf(3);
	public static final Integer ACCESS_LEVEL_DEPARTMENT = Integer.valueOf(2);
	// ----

	// ---- SUBQUERIES AND FIELDS
	public static Field<?>[] USERS_FIELDS = new Field<?>[] { USERS.USER_ID, USERS.USER_TYPE, USERS.USER_EMPL_FK, USERS.USER_SITE_FK, USERS.USER_DEPT_FK };
	public static final Field<Integer> LATEST_UPDATE = DSL.coalesce(DSL.select(UPDATES.UPDT_PK)
			.from(UPDATES).where(UPDATES.UPDT_DATE.eq(DSL.select(DSL.max(UPDATES.UPDT_DATE)).from(UPDATES))).asField(), NO_UPDATE);

	public static Field<Date> fieldDate(final String dateStr) {
		return dateStr != null ? DSL.date(dateStr) : DSL.currentDate();
	}

	/**
	 * Returns a sub-query selecting the <strong>primary key</strong> of the
	 * {@link Updates} that is or was relevant at a given date, or today if no
	 * date is specified.
	 *
	 * @param dateStr
	 *            The date for which to compute the relevant {@link Updates}, in
	 *            the <code>"YYYY-MM-DD"</code> format.
	 * @return The relevant {@link Updates}'s primary key or
	 *         {@value ResourcesEndpoint.NO_UPDATE} if no such update found.
	 */
	public static Field<Integer> selectUpdate(final String dateStr) {
		return DSL.coalesce(
							DSL.select(UPDATES.UPDT_PK).from(UPDATES).where(UPDATES.UPDT_DATE.eq(DSL.select(DSL.max(UPDATES.UPDT_DATE)).from(UPDATES)
									.where(UPDATES.UPDT_DATE.le(Constants.fieldDate(dateStr)))))
									.asField(),
							NO_UPDATE);
	}

	// -- TRAININGS STATISTICS
	public static final Field<Integer> TRAINING_REGISTERED = DSL.count(TRAININGS_EMPLOYEES.TREM_PK).as("registered");
	public static final Field<Integer> TRAINING_VALIDATED = DSL.count(TRAININGS_EMPLOYEES.TREM_OUTCOME)
			.filterWhere(TRAININGS_EMPLOYEES.TREM_OUTCOME.eq(EMPL_OUTCOME_VALIDATED)).as("validated");
	public static final Field<Integer> TRAINING_FLUNKED = DSL.count(TRAININGS_EMPLOYEES.TREM_OUTCOME)
			.filterWhere(TRAININGS_EMPLOYEES.TREM_OUTCOME.eq(EMPL_OUTCOME_FLUNKED)).as("flunked");
	public static final Field<String> TRAINING_TRAINERS = DSL.select(DSL.arrayAgg(TRAININGS_TRAINERS.TRTR_EMPL_FK)).from(TRAININGS_TRAINERS)
			.where(TRAININGS_TRAINERS.TRTR_TRNG_FK.eq(TRAININGS.TRNG_PK)).asField("trainers");
	public static final Field<Date> TRAINING_EXPIRY = DSL
			.dateAdd(
						TRAININGS.TRNG_DATE,
						DSL.field(DSL.select(TRAININGTYPES.TRTY_VALIDITY).from(TRAININGTYPES).where(TRAININGTYPES.TRTY_PK.eq(TRAININGS.TRNG_TRTY_FK))),
						DatePart.MONTH)
			.as("expiry");
	// --

	// -- EMPLOYEES STATISTICS
	public static final Field<Date> EXPIRY = DSL
			.when(
					EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_DATE.le(DSL.max(TRAININGS.TRNG_DATE.plus(TRAININGTYPES.TRTY_VALIDITY.mul(new YearToMonth(0, 1))))),
					EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_DATE)
			.otherwise(DSL.max(TRAININGS.TRNG_DATE.plus(TRAININGTYPES.TRTY_VALIDITY.mul(new YearToMonth(0, 1)))));

	public static Field<String> fieldValidity(final String dateStr) {
		return DSL
				.when(
						EXPIRY.ge(Constants.fieldDate(dateStr).plus(new YearToMonth(0, 6))),
						Constants.STATUS_SUCCESS)
				.when(
						EXPIRY.ge(Constants.fieldDate(dateStr)),
						Constants.STATUS_WARNING)
				.otherwise(Constants.STATUS_DANGER);
	}

	/**
	 * The <code>Condition</code> should be on
	 * <code>TRAININGS_EMPLOYEES.TREM_EMPL_FK</code>.
	 */
	public static Select<Record5<String, String, Integer, Date, String>> selectEmployeesStats(final String dateStr, final Condition employeesSelection) {
		return DSL
				.select(
						SITES_EMPLOYEES.SIEM_SITE_FK,
						TRAININGS_EMPLOYEES.TREM_EMPL_FK,
						TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK,
						Constants.EXPIRY.as("expiry"),
						Constants.fieldValidity(dateStr).as("validity"))
				.from(TRAININGTYPES_CERTIFICATES)
				.join(TRAININGTYPES).on(TRAININGTYPES.TRTY_PK.eq(TRAININGTYPES_CERTIFICATES.TTCE_TRTY_FK))
				.join(TRAININGS).on(TRAININGS.TRNG_TRTY_FK.eq(TRAININGTYPES.TRTY_PK))
				.join(TRAININGS_EMPLOYEES).on(TRAININGS_EMPLOYEES.TREM_TRNG_FK.eq(TRAININGS.TRNG_PK))
				.leftJoin(EMPLOYEES_CERTIFICATES_OPTOUT)
				.on(EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_EMPL_FK.eq(TRAININGS_EMPLOYEES.TREM_EMPL_FK)
						.and(EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_CERT_FK.eq(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK)))
				.join(SITES_EMPLOYEES)
				.on(SITES_EMPLOYEES.SIEM_EMPL_FK.eq(TRAININGS_EMPLOYEES.TREM_EMPL_FK)
						.and(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(Constants.selectUpdate(dateStr))))
				.where(TRAININGS_EMPLOYEES.TREM_OUTCOME.eq(Constants.EMPL_OUTCOME_VALIDATED))
				.and(TRAININGS.TRNG_DATE.le(Constants.fieldDate(dateStr)))
				.and(employeesSelection)
				.groupBy(
							SITES_EMPLOYEES.SIEM_SITE_FK,
							TRAININGS_EMPLOYEES.TREM_EMPL_FK,
							TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK,
							EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_DATE);
	}

	/**
	 * The <code>employeesSelection</code> condition should be on
	 * <code>TRAININGS_EMPLOYEES.TREM_EMPL_FK</code>.<br />
	 * The <code>sitesSelection</code> condition should be on
	 * <code>SITES_EMPLOYEES.SIEM_SITE_FK</code>.
	 */
	@SuppressWarnings("null")
	public static Select<Record7<String, Integer, Integer, Integer, Integer, Integer, String>> selectSitesStats(
																												final String dateStr,
																												final Condition employeesSelection,
																												final Condition sitesSelection) {
		final Table<Record5<String, String, Integer, Date, String>> employeesStats = Constants
				.selectEmployeesStats(dateStr, employeesSelection)
				.asTable();

		final Table<Record5<String, Integer, Integer, Integer, Integer>> certificatesStats = DSL
				.select(
						employeesStats.field(SITES_EMPLOYEES.SIEM_SITE_FK),
						employeesStats.field(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK),
						DSL.count(employeesStats.field(TRAININGS_EMPLOYEES.TREM_EMPL_FK))
								.filterWhere(employeesStats.field("validity", String.class).eq(Constants.STATUS_SUCCESS))
								.as(Constants.STATUS_SUCCESS),
						DSL.count(employeesStats.field(TRAININGS_EMPLOYEES.TREM_EMPL_FK))
								.filterWhere(employeesStats.field("validity", String.class).eq(Constants.STATUS_WARNING))
								.as(Constants.STATUS_WARNING),
						DSL.count(employeesStats.field(TRAININGS_EMPLOYEES.TREM_EMPL_FK))
								.filterWhere(employeesStats.field("validity", String.class).eq(Constants.STATUS_DANGER))
								.as(Constants.STATUS_DANGER))
				.from(employeesStats)
				.groupBy(employeesStats.field(SITES_EMPLOYEES.SIEM_SITE_FK), employeesStats.field(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK))
				.asTable();

		final Table<Record4<String, Integer, Integer, Integer>> certificates = DSL
				.select(
						SITES_EMPLOYEES.SIEM_SITE_FK,
						CERTIFICATES.CERT_PK,
						CERTIFICATES.CERT_TARGET,
						DSL.count().as("employeesCount"))
				.from(SITES_EMPLOYEES)
				.rightJoin(CERTIFICATES).on(DSL.val(true))
				.where(sitesSelection)
				.and(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(Constants.selectUpdate(dateStr)))
				.groupBy(SITES_EMPLOYEES.SIEM_SITE_FK, CERTIFICATES.CERT_PK, CERTIFICATES.CERT_TARGET)
				.asTable();

		final Field<Integer> targetCount = DSL.ceil(certificates.field("employeesCount", Integer.class)
				.mul(certificates.field(CERTIFICATES.CERT_TARGET).div(DSL.val(100f))));
		final Field<Integer> validCount = DSL
				.coalesce(
							certificatesStats.field(Constants.STATUS_SUCCESS, Integer.class).add(certificatesStats.field(Constants.STATUS_WARNING)),
							Integer.valueOf(0));
		return DSL.select(
							certificates.field(SITES_EMPLOYEES.SIEM_SITE_FK),
							certificates.field(CERTIFICATES.CERT_PK),
							DSL.coalesce(certificatesStats.field(Constants.STATUS_SUCCESS, Integer.class), Integer.valueOf(0)).as(Constants.STATUS_SUCCESS),
							DSL.coalesce(certificatesStats.field(Constants.STATUS_WARNING, Integer.class), Integer.valueOf(0)).as(Constants.STATUS_WARNING),
							DSL.coalesce(certificatesStats.field(Constants.STATUS_DANGER, Integer.class), Integer.valueOf(0)).as(Constants.STATUS_DANGER),
							targetCount.as("target"),
							DSL
									.when(validCount.ge(targetCount), Constants.STATUS_SUCCESS)
									.when(validCount.ge(DSL.floor(targetCount.mul(DSL.val(2 / 3f)))), Constants.STATUS_WARNING)
									.otherwise(Constants.STATUS_DANGER).as("validity"))
				.from(certificatesStats)
				.rightJoin(certificates)
				.on(certificates.field(CERTIFICATES.CERT_PK).eq(certificatesStats.field(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK))
						.and(certificates.field(SITES_EMPLOYEES.SIEM_SITE_FK).eq(certificatesStats.field(SITES_EMPLOYEES.SIEM_SITE_FK))));
	}
}
