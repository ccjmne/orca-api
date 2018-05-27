package org.ccjmne.orca.api.demo;

import static org.ccjmne.orca.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS_TRAINERS;

import java.sql.Date;
import java.util.Arrays;

import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.jooq.classes.tables.records.TrainingsRecord;
import org.jooq.DSLContext;
import org.jooq.InsertOnDuplicateStep;
import org.jooq.Record4;
import org.jooq.Row1;
import org.jooq.Select;
import org.jooq.Table;
import org.jooq.impl.DSL;

public class DemoDataTrainings {

	public static void generate(final DSLContext ctx) {
		ctx.execute(DemoDataTrainings.addTrainings(Constants.EMPL_OUTCOME_CANCELLED, 50));
		ctx.execute(DemoDataTrainings.addTrainings(Constants.TRNG_OUTCOME_COMPLETED, 442));
		ctx.execute(DemoDataTrainings.addTrainings(Constants.TRNG_OUTCOME_SCHEDULED, 8));

		// Adding trainers - 1% of employees are trainers
		ctx.insertInto(TRAININGS_TRAINERS, TRAININGS_TRAINERS.TRTR_TRNG_FK, TRAININGS_TRAINERS.TRTR_EMPL_FK)
				.select(DSL.select(DSL.field("trng_pk", Integer.class), DSL.field("empl_pk", Integer.class))
						.from(DSL.select(
											TRAININGS.TRNG_PK,
											DSL.floor(DSL.rand().mul(DSL.select(DSL.count()).from(EMPLOYEES).asField()).div(DSL.val(100))).as("linked_empl_id"))
								.from(TRAININGS).asTable("trainings_view"))
						.join(DSL.select(
											EMPLOYEES.EMPL_PK,
											DSL.rowNumber().over().orderBy(DSL.rand()).as("empl_id"))
								.from(EMPLOYEES).where(EMPLOYEES.EMPL_PK.ne(Constants.EMPLOYEE_ROOT)).asTable("employees_view"))
						.on(DSL.field("linked_empl_id").eq(DSL.field("empl_id"))))
				.onDuplicateKeyIgnore()
				.execute();

		// Adding VALIDATED employees - 1/4 or them, five times
		for (int i = 0; i < 4; i++) {
			ctx.insertInto(TRAININGS_EMPLOYEES, TRAININGS_EMPLOYEES.TREM_TRNG_FK, TRAININGS_EMPLOYEES.TREM_EMPL_FK, TRAININGS_EMPLOYEES.TREM_OUTCOME)
					.select(DSL.select(DSL.field("trng_pk", Integer.class), DSL.field("empl_pk", Integer.class), DSL.val(Constants.EMPL_OUTCOME_VALIDATED))
							.from(DSL.select(
												EMPLOYEES.EMPL_PK,
												DSL.floor(DSL.rand().mul(DSL.select(DSL.count()).from(TRAININGS)
														.where(TRAININGS.TRNG_OUTCOME.eq(Constants.TRNG_OUTCOME_COMPLETED))
														.asField()).mul(DSL.val(4)))
														.as("linked_trng_id"))
									.from(EMPLOYEES).where(EMPLOYEES.EMPL_PK.ne(Constants.EMPLOYEE_ROOT)).asTable("employees_view"))
							.join(DSL.select(
												TRAININGS.TRNG_PK,
												DSL.rowNumber().over().orderBy(DSL.rand()).as("trng_id"))
									.from(TRAININGS).where(TRAININGS.TRNG_OUTCOME.eq(Constants.TRNG_OUTCOME_COMPLETED)).asTable("trainings_view"))
							.on(DSL.field("linked_trng_id").eq(DSL.field("trng_id"))))
					.onDuplicateKeyIgnore()
					.execute();
		}

		// Adding FLUNKED employees - 1/4 of them
		final Row1<String>[] flunkedComments = Arrays.asList(
																"En incapacité physique d'effectuer certains gestes techniques",
																"A quitté la formation avant la fin")
				.stream().map(DSL::row).toArray(Row1[]::new);

		ctx.insertInto(
						TRAININGS_EMPLOYEES,
						TRAININGS_EMPLOYEES.TREM_TRNG_FK,
						TRAININGS_EMPLOYEES.TREM_EMPL_FK,
						TRAININGS_EMPLOYEES.TREM_COMMENT,
						TRAININGS_EMPLOYEES.TREM_OUTCOME)
				.select(DSL.select(
									DSL.field("trng_pk", Integer.class),
									DSL.field("empl_pk", Integer.class),
									DSL.field("trem_comment", String.class),
									DSL.val(Constants.EMPL_OUTCOME_FLUNKED))
						.from(DSL.select(
											EMPLOYEES.EMPL_PK,
											DSL.floor(DSL.rand().mul(DSL.select(DSL.count()).from(TRAININGS)
													.where(TRAININGS.TRNG_OUTCOME.eq(Constants.TRNG_OUTCOME_COMPLETED))
													.asField()).mul(DSL.val(4))).as("linked_trng_id"),
											DSL.ceil(DSL.rand().mul(Integer.valueOf(flunkedComments.length))).as("linked_trem_comment_id"))
								.from(EMPLOYEES).where(EMPLOYEES.EMPL_PK.ne(Constants.EMPLOYEE_ROOT)).asTable("employees_view"))
						.join(DSL.select(
											TRAININGS.TRNG_PK,
											DSL.rowNumber().over().orderBy(DSL.rand()).as("trng_id"))
								.from(TRAININGS).where(TRAININGS.TRNG_OUTCOME.eq(Constants.TRNG_OUTCOME_COMPLETED)).asTable("trainings_view"))
						.on(DSL.field("linked_trng_id").eq(DSL.field("trng_id")))
						.join(DSL.select(
											DSL.field("trem_comment"),
											DSL.rowNumber().over().as("trem_comment_id"))
								.from(DSL.values(flunkedComments).as("unused", "trem_comment")))
						.on(DSL.field("linked_trem_comment_id").eq(DSL.field("trem_comment_id"))))
				.onDuplicateKeyIgnore()
				.execute();

		// Adding MISSING employees - 1/4 of them
		final Row1<String>[] missingComments = Arrays.asList("Absent(e)", "En congés", "A été muté(e)").stream().map(DSL::row).toArray(Row1[]::new);
		ctx.insertInto(	TRAININGS_EMPLOYEES,
						TRAININGS_EMPLOYEES.TREM_TRNG_FK,
						TRAININGS_EMPLOYEES.TREM_EMPL_FK,
						TRAININGS_EMPLOYEES.TREM_COMMENT,
						TRAININGS_EMPLOYEES.TREM_OUTCOME)
				.select(DSL
						.select(DSL.field("trng_pk", Integer.class), DSL.field("empl_pk", Integer.class), DSL.field("trem_comment", String.class),
								DSL.val(Constants.EMPL_OUTCOME_MISSING))
						.from(DSL.select(
											EMPLOYEES.EMPL_PK,
											DSL.floor(DSL.rand().mul(DSL.select(DSL.count())
													.from(TRAININGS).where(TRAININGS.TRNG_OUTCOME.eq(Constants.TRNG_OUTCOME_COMPLETED))
													.asField()).mul(DSL.val(7)))
													.as("linked_trng_id"),
											DSL.ceil(DSL.rand().mul(Integer.valueOf(missingComments.length))).as("linked_trem_comment_id"))
								.from(EMPLOYEES).where(EMPLOYEES.EMPL_PK.ne(Constants.EMPLOYEE_ROOT)).asTable("employees_view"))
						.join(DSL.select(
											TRAININGS.TRNG_PK,
											DSL.rowNumber().over().orderBy(DSL.rand()).as("trng_id"))
								.from(TRAININGS).where(TRAININGS.TRNG_OUTCOME.eq(Constants.TRNG_OUTCOME_COMPLETED)).asTable("trainings_view"))
						.on(DSL.field("linked_trng_id").eq(DSL.field("trng_id")))
						.join(DSL.select(
											DSL.field("trem_comment"),
											DSL.rowNumber().over().as("trem_comment_id"))
								.from(DSL.values(missingComments).as("unused", "trem_comment")))
						.on(DSL.field("linked_trem_comment_id").eq(DSL.field("trem_comment_id"))))
				.onDuplicateKeyIgnore()
				.execute();

		// Adding PENDING employees - 1/35 of them
		ctx.insertInto(TRAININGS_EMPLOYEES, TRAININGS_EMPLOYEES.TREM_TRNG_FK, TRAININGS_EMPLOYEES.TREM_EMPL_FK, TRAININGS_EMPLOYEES.TREM_OUTCOME)
				.select(DSL.select(DSL.field("trng_pk", Integer.class), DSL.field("empl_pk", Integer.class), DSL.val(Constants.EMPL_OUTCOME_PENDING))
						.from(DSL.select(
											EMPLOYEES.EMPL_PK,
											DSL.floor(DSL.rand().mul(DSL.select(DSL.count())
													.from(TRAININGS).where(TRAININGS.TRNG_OUTCOME.eq(Constants.TRNG_OUTCOME_SCHEDULED))
													.asField()).mul(DSL.val(35)))
													.as("linked_trng_id"))
								.from(EMPLOYEES).where(EMPLOYEES.EMPL_PK.ne(Constants.EMPLOYEE_ROOT)).asTable("employees_view"))
						.join(DSL.select(
											TRAININGS.TRNG_PK,
											DSL.rowNumber().over().orderBy(DSL.rand()).as("trng_id"))
								.from(TRAININGS).where(TRAININGS.TRNG_OUTCOME.eq(Constants.TRNG_OUTCOME_SCHEDULED)).asTable("trainings_view"))
						.on(DSL.field("linked_trng_id").eq(DSL.field("trng_id"))))
				.onDuplicateKeyIgnore()
				.execute();

		// Adding CANCELLED employees - 1/7 of them
		ctx.insertInto(TRAININGS_EMPLOYEES, TRAININGS_EMPLOYEES.TREM_TRNG_FK, TRAININGS_EMPLOYEES.TREM_EMPL_FK, TRAININGS_EMPLOYEES.TREM_OUTCOME)
				.select(DSL.select(DSL.field("trng_pk", Integer.class), DSL.field("empl_pk", Integer.class), DSL.val(Constants.EMPL_OUTCOME_CANCELLED))
						.from(DSL.select(
											EMPLOYEES.EMPL_PK,
											DSL.floor(DSL.rand().mul(DSL.select(DSL.count())
													.from(TRAININGS).where(TRAININGS.TRNG_OUTCOME.eq(Constants.TRNG_OUTCOME_CANCELLED))
													.asField()).mul(DSL.val(35)))
													.as("linked_trng_id"))
								.from(EMPLOYEES).where(EMPLOYEES.EMPL_PK.ne(Constants.EMPLOYEE_ROOT)).asTable("employees_view"))
						.join(DSL.select(
											TRAININGS.TRNG_PK,
											DSL.rowNumber().over().orderBy(DSL.rand()).as("trng_id"))
								.from(TRAININGS).where(TRAININGS.TRNG_OUTCOME.eq(Constants.TRNG_OUTCOME_CANCELLED)).asTable("trainings_view"))
						.on(DSL.field("linked_trng_id").eq(DSL.field("trng_id"))))
				.onDuplicateKeyIgnore()
				.execute();
	}

	@SuppressWarnings("unchecked")
	private static InsertOnDuplicateStep<TrainingsRecord> addTrainings(final String outcome, final int amount) {
		final Table<Record4<Integer, Date, String, String>> sessions = FakeRecords.randomSessions(outcome, amount);
		return DSL.insertInto(TRAININGS, TRAININGS.TRNG_TRTY_FK, TRAININGS.TRNG_DATE, TRAININGS.TRNG_OUTCOME, TRAININGS.TRNG_COMMENT)
				.select((Select<? extends Record4<Integer, Date, String, String>>) DSL.select(sessions.fields()).from(sessions));
	}
}
