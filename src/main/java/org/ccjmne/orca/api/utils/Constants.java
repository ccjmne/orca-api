package org.ccjmne.orca.api.utils;

import static org.ccjmne.orca.jooq.classes.Tables.CERTIFICATES;
import static org.ccjmne.orca.jooq.classes.Tables.DEPARTMENTS;
import static org.ccjmne.orca.jooq.classes.Tables.EMPLOYEES_CERTIFICATES_OPTOUT;
import static org.ccjmne.orca.jooq.classes.Tables.SITES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS_TRAINERS;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGTYPES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGTYPES_CERTIFICATES;
import static org.ccjmne.orca.jooq.classes.Tables.UPDATES;
import static org.ccjmne.orca.jooq.classes.Tables.USERS;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.time.Month;
import java.time.temporal.TemporalAdjusters;
import java.util.Arrays;
import java.util.List;

import org.ccjmne.orca.jooq.classes.tables.records.UpdatesRecord;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Record10;
import org.jooq.Record4;
import org.jooq.Record5;
import org.jooq.Record6;
import org.jooq.Record8;
import org.jooq.Select;
import org.jooq.SelectQuery;
import org.jooq.Table;
import org.jooq.TableLike;
import org.jooq.impl.DSL;
import org.jooq.types.DayToSecond;
import org.jooq.types.YearToMonth;

public class Constants {

	/**
	 * If you are trying to use the maximum value as some kind of flag such as
	 * "undetermined future date" to avoid a NULL, instead choose some arbitrary
	 * date far enough in the future to exceed any legitimate value but not so
	 * far as to exceed the limits of any database you are possibly going to
	 * use. Define a constant for this value in your Java code and in your
	 * database, and document thoroughly.
	 *
	 * @see <a href=
	 *      "http://stackoverflow.com/questions/41301892/insert-the-max-date-independent-from-database">
	 *      Insert the max date (independent from database)</a>
	 */
	public static final Date DATE_INFINITY = Date.valueOf(LocalDate.of(9999, Month.JANUARY, 1).with(TemporalAdjusters.lastDayOfYear()));

	// ---- API CONSTANTS
	public static final String FIELDS_ALL = "all";
	public static final String DATE_INFINITY_LITERAL = "infinity";
	private static final Integer DURATION_INFINITE = Integer.valueOf(0);
	private static final Integer NO_UPDATE = Integer.valueOf(-1);

	public static final String STATUS_SUCCESS = "success";
	public static final String STATUS_WARNING = "warning";
	public static final String STATUS_DANGER = "danger";
	// ----

	// ---- DATABASE CONSTANTS
	public static final String TRNG_OUTCOME_CANCELLED = "CANCELLED";
	public static final String TRNG_OUTCOME_COMPLETED = "COMPLETED";
	public static final String TRNG_OUTCOME_SCHEDULED = "SCHEDULED";
	public static final List<String> TRAINING_OUTCOMES = Arrays
			.asList(Constants.TRNG_OUTCOME_CANCELLED, Constants.TRNG_OUTCOME_COMPLETED, Constants.TRNG_OUTCOME_SCHEDULED);

	public static final String EMPL_OUTCOME_CANCELLED = "CANCELLED";
	public static final String EMPL_OUTCOME_FLUNKED = "FLUNKED";
	public static final String EMPL_OUTCOME_PENDING = "PENDING";
	public static final String EMPL_OUTCOME_VALIDATED = "VALIDATED";

	public static final String USER_ROOT = "root";

	public static final String UNASSIGNED_SITE = "0";
	public static final Integer UNASSIGNED_DEPARTMENT = Integer.valueOf(0);
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
	public static final Field<Integer> CURRENT_UPDATE = Constants.selectUpdate(null);

	/**
	 * Returns a select sub-query that maps the results of the provided
	 * {@code query} to the sole specified {@code field}.
	 *
	 * @param <T>
	 *            The specified field type.
	 * @param field
	 *            The field to extract.
	 * @param query
	 *            The original query to use as a data source.
	 * @return A sub-query containing the sole specified {@code field} from the
	 *         given {@code query}.
	 */
	public static <T> Select<Record1<T>> select(final Field<T> field, final SelectQuery<? extends Record> query) {
		final TableLike<? extends Record> table = query.asTable();
		return DSL.select(table.field(field)).from(table);
	}

	private static Field<Date> fieldDate(final String dateStr) {
		return dateStr != null ? DSL.date(dateStr) : DSL.currentDate();
	}

	/**
	 * Returns a sub-query selecting the <strong>primary key</strong> of the
	 * {@link UpdatesRecord} that is or was relevant at a given date, or today
	 * if no date is specified.
	 *
	 * @param dateStr
	 *            The date for which to compute the relevant
	 *            {@link UpdatesRecord}, in the <code>"YYYY-MM-DD"</code>
	 *            format.
	 * @return The relevant {@link UpdatesRecord}'s primary key or
	 *         {@value Constants#NO_UPDATE} if no such update found.
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
	// --

	// -- EMPLOYEES, SITES AND DEPARTMENTS STATISTICS
	private static final Field<Date> MAX_EXPIRY = DSL.max(DSL
			.when(TRAININGTYPES_CERTIFICATES.TTCE_DURATION.eq(Constants.DURATION_INFINITE), DSL.date(Constants.DATE_INFINITY))
			.otherwise(TRAININGS.TRNG_DATE.plus(TRAININGTYPES_CERTIFICATES.TTCE_DURATION.mul(new YearToMonth(0, 1)))));

	private static final Field<Date> EXPIRY = DSL
			.when(
					EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_DATE.le(Constants.MAX_EXPIRY),
					EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_DATE.sub(new DayToSecond(1)))
			.otherwise(Constants.MAX_EXPIRY);

	private static Field<String> fieldValidity(final String dateStr) {
		return DSL
				.when(
						Constants.EXPIRY.ge(Constants.fieldDate(dateStr).plus(new YearToMonth(0, 6))),
						Constants.STATUS_SUCCESS)
				.when(
						Constants.EXPIRY.ge(Constants.fieldDate(dateStr)),
						Constants.STATUS_WARNING)
				.otherwise(Constants.STATUS_DANGER);
	}

	private static Field<Date> fieldOptedOut(final String dateStr) {
		return DSL.field(EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_DATE);
	}

	// TODO: move everything below in StatisticsEndpoint?

	/**
	 * The <code>Condition</code> should be on
	 * <code>TRAININGS_EMPLOYEES.TREM_EMPL_FK</code>.
	 */
	public static Select<Record6<String, String, Integer, Date, Date, String>> selectEmployeesStats(
																									final String dateStr,
																									final Condition employeesSelection) {
		return DSL
				.select(
						SITES_EMPLOYEES.SIEM_SITE_FK,
						TRAININGS_EMPLOYEES.TREM_EMPL_FK,
						TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK,
						Constants.EXPIRY.as("expiry"),
						Constants.fieldOptedOut(dateStr).as("opted_out"),
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
	public static Select<Record8<Integer, String, Integer, Integer, Integer, Integer, Integer, String>> selectSitesStats(
																															final String dateStr,
																															final Condition employeesSelection,
																															final Condition sitesSelection) {
		final Table<Record6<String, String, Integer, Date, Date, String>> employeesStats = Constants
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
		final Field<Integer> warningTargetCount = DSL.ceil(certificates.field("employeesCount", Integer.class)
				.mul(certificates.field(CERTIFICATES.CERT_TARGET).div(DSL.val(300 / 2f))));
		final Field<Integer> validCount = DSL
				.coalesce(
							certificatesStats.field(Constants.STATUS_SUCCESS, Integer.class).add(certificatesStats.field(Constants.STATUS_WARNING)),
							Integer.valueOf(0));
		return DSL.select(
							SITES.SITE_DEPT_FK,
							certificates.field(SITES_EMPLOYEES.SIEM_SITE_FK),
							certificates.field(CERTIFICATES.CERT_PK),
							DSL.coalesce(certificatesStats.field(Constants.STATUS_SUCCESS, Integer.class), Integer.valueOf(0)).as(Constants.STATUS_SUCCESS),
							DSL.coalesce(certificatesStats.field(Constants.STATUS_WARNING, Integer.class), Integer.valueOf(0)).as(Constants.STATUS_WARNING),
							DSL.coalesce(certificatesStats.field(Constants.STATUS_DANGER, Integer.class), Integer.valueOf(0)).as(Constants.STATUS_DANGER),
							targetCount.as("target"),
							DSL
									.when(validCount.ge(targetCount), Constants.STATUS_SUCCESS)
									.when(validCount.ge(warningTargetCount), Constants.STATUS_WARNING)
									.otherwise(Constants.STATUS_DANGER).as("validity"))
				.from(certificatesStats)
				.rightJoin(certificates)
				.on(certificates.field(CERTIFICATES.CERT_PK).eq(certificatesStats.field(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK))
						.and(certificates.field(SITES_EMPLOYEES.SIEM_SITE_FK).eq(certificatesStats.field(SITES_EMPLOYEES.SIEM_SITE_FK))))
				.join(SITES).on(SITES.SITE_PK.eq(certificates.field(SITES_EMPLOYEES.SIEM_SITE_FK)));
	}

	/**
	 * The <code>employeesSelection</code> condition should be on
	 * <code>TRAININGS_EMPLOYEES.TREM_EMPL_FK</code>.<br />
	 * The <code>sitesSelection</code> condition should be on
	 * <code>SITES_EMPLOYEES.SIEM_SITE_FK</code>.<br />
	 * The <code>departmentsSelection</code> condition should be on
	 * <code>DEPARTMENTS.DEPT_PK</code>.
	 */
	@SuppressWarnings("null")
	public static Select<Record10<Integer, Integer, BigDecimal, BigDecimal, BigDecimal, Integer, Integer, Integer, BigDecimal, String>> selectDepartmentsStats(
																																								final String dateStr,
																																								final Condition employeesSelection,
																																								final Condition sitesSelection,
																																								final Condition departmentsSelection) {
		final Table<Record8<Integer, String, Integer, Integer, Integer, Integer, Integer, String>> sitesStats = Constants
				.selectSitesStats(
									dateStr,
									employeesSelection,
									sitesSelection)
				.asTable();

		final Field<BigDecimal> score = DSL.round(DSL.sum(DSL
				.when(sitesStats.field("validity", String.class).eq(Constants.STATUS_SUCCESS), DSL.val(1f))
				.when(sitesStats.field("validity", String.class).eq(Constants.STATUS_WARNING), DSL.val(2 / 3f))
				.otherwise(DSL.val(0f))).mul(DSL.val(100)).div(DSL.count()));

		return DSL.select(
							DEPARTMENTS.DEPT_PK,
							sitesStats.field(CERTIFICATES.CERT_PK),
							DSL.sum(sitesStats.field(Constants.STATUS_SUCCESS, Integer.class)).as(Constants.STATUS_SUCCESS),
							DSL.sum(sitesStats.field(Constants.STATUS_WARNING, Integer.class)).as(Constants.STATUS_WARNING),
							DSL.sum(sitesStats.field(Constants.STATUS_DANGER, Integer.class)).as(Constants.STATUS_DANGER),
							DSL.count().filterWhere(sitesStats.field("validity", String.class).eq(Constants.STATUS_SUCCESS))
									.as("sites_" + Constants.STATUS_SUCCESS),
							DSL.count().filterWhere(sitesStats.field("validity", String.class).eq(Constants.STATUS_WARNING))
									.as("sites_" + Constants.STATUS_WARNING),
							DSL.count().filterWhere(sitesStats.field("validity", String.class).eq(Constants.STATUS_DANGER))
									.as("sites_" + Constants.STATUS_DANGER),
							score.as("score"),
							DSL.when(score.eq(DSL.val(BigDecimal.valueOf(100))), Constants.STATUS_SUCCESS)
									.when(score.ge(DSL.val(BigDecimal.valueOf(67))), Constants.STATUS_WARNING)
									.otherwise(Constants.STATUS_DANGER).as("validity"))
				.from(sitesStats)
				.join(DEPARTMENTS).on(DEPARTMENTS.DEPT_PK.eq(sitesStats.field(SITES.SITE_DEPT_FK)))
				.where(departmentsSelection)
				.groupBy(DEPARTMENTS.DEPT_PK, sitesStats.field(CERTIFICATES.CERT_PK));
	}
}
