package org.ccjmne.faomaintenance.api.utils;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGTYPES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.UPDATES;

import java.sql.Date;

import org.jooq.DatePart;
import org.jooq.Field;
import org.jooq.impl.DSL;

public class Constants {
	public static final String STATUS_SUCCESS = "success";
	public static final String STATUS_WARNING = "warning";
	public static final String STATUS_DANGER = "danger";

	// ---- DATABASE CONSTANTS
	public static final String TRNG_OUTCOME_COMPLETED = "COMPLETED";
	public static final String EMPL_OUTCOME_VALIDATED = "VALIDATED";

	public static final String USER_ROOT = "admin";

	public static final String SITE_UNASSIGNED = "0";
	public static final Integer DEPT_UNASSIGNED = Integer.valueOf(0);

	public static final String ROLE_ACCESS = "access";
	public static final String ROLE_TRAINER = "trainer";
	public static final String ROLE_ADMIN = "admin";

	public static final Integer TRAININGS_ACCESS = Integer.valueOf(4);
	public static final Integer ALL_SITES_ACCESS = Integer.valueOf(3);
	public static final Integer ONLY_DEPT_ACCESS = Integer.valueOf(2);
	// DATABASE CONSTANTS ----

	// ---- SUBQUERIES AND FIELDS
	public static final Field<Integer> LATEST_UPDATE = DSL.select(UPDATES.UPDT_PK)
			.from(UPDATES).where(UPDATES.UPDT_DATE.eq(DSL.select(DSL.max(UPDATES.UPDT_DATE)).from(UPDATES))).asField();

	public static final Field<Integer> AGENTS_REGISTERED = DSL.count(TRAININGS_EMPLOYEES.TREM_PK).as("registered");
	public static final Field<Integer> AGENTS_VALIDATED = DSL.count(TRAININGS_EMPLOYEES.TREM_OUTCOME)
			.filterWhere(TRAININGS_EMPLOYEES.TREM_OUTCOME.eq("VALIDATED")).as("validated");
	public static final Field<Date> EXPIRY_DATE = DSL
			.dateAdd(
						TRAININGS.TRNG_DATE,
						DSL.field(DSL.select(TRAININGTYPES.TRTY_VALIDITY).from(TRAININGTYPES).where(TRAININGTYPES.TRTY_PK.eq(TRAININGS.TRNG_TRTY_FK))),
						DatePart.MONTH);
	// SUBQUERIES AND FIELDS ----
}
