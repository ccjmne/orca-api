package org.ccjmne.faomaintenance.api.utils;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS_TRAINERS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGTYPES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGTYPES_CERTIFICATES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.UPDATES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.USERS;

import java.sql.Date;
import java.text.ParseException;

import org.ccjmne.faomaintenance.jooq.classes.tables.Updates;
import org.jooq.Condition;
import org.jooq.DatePart;
import org.jooq.Field;
import org.jooq.Record4;
import org.jooq.SelectHavingStep;
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
	 * @throws ParseException
	 *             if the <code>dateStr</code> attribute can't be parsed.
	 */
	public static Field<Integer> selectUpdate(final String dateStr) throws ParseException {
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
	public static final Field<Date> EXPIRY = DSL.max(TRAININGS.TRNG_DATE.plus(TRAININGTYPES.TRTY_VALIDITY.mul(new YearToMonth(0, 1))));

	public static Field<String> fieldValidity(final String dateStr) {
		return DSL
				.when(
						DSL.max(TRAININGS.TRNG_DATE.plus(TRAININGTYPES.TRTY_VALIDITY.mul(new YearToMonth(0, 1))))
								.ge(Constants.fieldDate(dateStr).plus(new YearToMonth(0, 6))),
						Constants.STATUS_SUCCESS)
				.when(
						DSL.max(TRAININGS.TRNG_DATE.plus(TRAININGTYPES.TRTY_VALIDITY.mul(new YearToMonth(0, 1))))
								.ge(Constants.fieldDate(dateStr)),
						Constants.STATUS_WARNING)
				.otherwise(Constants.STATUS_DANGER);
	}

	public static SelectHavingStep<Record4<String, Integer, Date, String>> selectEmployeesStats(final String dateStr, final Condition employeesSelection) {
		return DSL
				.select(
						TRAININGS_EMPLOYEES.TREM_EMPL_FK,
						TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK,
						Constants.EXPIRY.as("expiry"),
						Constants.fieldValidity(dateStr).as("validity"))
				.from(TRAININGTYPES_CERTIFICATES)
				.join(TRAININGTYPES).on(TRAININGTYPES.TRTY_PK.eq(TRAININGTYPES_CERTIFICATES.TTCE_TRTY_FK))
				.join(TRAININGS).on(TRAININGS.TRNG_TRTY_FK.eq(TRAININGTYPES.TRTY_PK))
				.join(TRAININGS_EMPLOYEES).on(TRAININGS_EMPLOYEES.TREM_TRNG_FK.eq(TRAININGS.TRNG_PK))
				.where(TRAININGS_EMPLOYEES.TREM_OUTCOME.eq(Constants.EMPL_OUTCOME_VALIDATED))
				.and(TRAININGS.TRNG_DATE.le(Constants.fieldDate(dateStr)))
				.and(employeesSelection)
				.groupBy(TRAININGS_EMPLOYEES.TREM_EMPL_FK, TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK);
	}
}
