package org.ccjmne.faomaintenance.api.utils;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS_TRAINERS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGTYPES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.UPDATES;

import java.sql.Date;

import org.ccjmne.faomaintenance.jooq.classes.tables.records.EmployeesRolesRecord;
import org.jooq.DatePart;
import org.jooq.Field;
import org.jooq.RecordMapper;
import org.jooq.impl.DSL;

public class Constants {
	public static final String STATUS_SUCCESS = "success";
	public static final String STATUS_WARNING = "warning";
	public static final String STATUS_DANGER = "danger";

	// ---- DATABASE CONSTANTS
	public static final String TRNG_OUTCOME_COMPLETED = "COMPLETED";
	public static final String EMPL_OUTCOME_VALIDATED = "VALIDATED";
	private static final String EMPL_OUTCOME_FLUNKED = "FLUNKED";

	public static final String USER_ROOT = "admin";

	public static final String UNASSIGNED_SITE = "0";
	public static final Integer UNASSIGNED_DEPT = Integer.valueOf(0);
	public static final Integer UNASSIGNED_TRAINERLEVEL = Integer.valueOf(0);

	public static final String ROLE_ACCESS = "access";
	public static final String ROLE_TRAINER = "trainer";
	public static final String ROLE_ADMIN = "admin";

	public static final Integer ACCESS_LEVEL_TRAININGS = Integer.valueOf(4);
	public static final Integer ACCESS_LEVEL_ALL_SITES = Integer.valueOf(3);
	public static final Integer ACCESS_LEVEL_ONE_DEPT = Integer.valueOf(2);
	// ----

	// ---- SUBQUERIES AND FIELDS
	public static final Field<Integer> LATEST_UPDATE = DSL.select(UPDATES.UPDT_PK)
			.from(UPDATES).where(UPDATES.UPDT_DATE.eq(DSL.select(DSL.max(UPDATES.UPDT_DATE)).from(UPDATES))).asField();

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
	public static final RecordMapper<EmployeesRolesRecord, Object> EMPLOYEES_ROLES_MAPPER = entry -> {
		switch (entry.getEmroType()) {
			case Constants.ROLE_ACCESS:
			case Constants.ROLE_ADMIN:
				return entry.getEmroLevel();
			case Constants.ROLE_TRAINER:
				return entry.getEmroTrlvFk();
			default:
				return Boolean.TRUE;
		}
	};
	// ----
}
