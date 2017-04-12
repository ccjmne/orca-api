package org.ccjmne.faomaintenance.api.demo;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS_TRAINERS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGTYPES;

import java.sql.Date;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.ccjmne.faomaintenance.api.utils.Constants;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.TrainingsRecord;
import org.joda.time.DateTime;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Insert;
import org.jooq.InsertValuesStep4;
import org.jooq.Row1;
import org.jooq.Table;
import org.jooq.impl.DSL;

import io.codearte.jfairy.Fairy;

public class DemoDataTrainings {

	public static final Fairy FAIRY = Fairy.create(Locale.FRENCH);

	@SuppressWarnings("unchecked")
	public static void generate(final DSLContext ctx) {
		addCompletedTrainings(
								ctx.insertInto(TRAININGS, TRAININGS.TRNG_DATE, TRAININGS.TRNG_START, TRAININGS.TRNG_TRTY_FK, TRAININGS.TRNG_OUTCOME),
								500)
										.execute();
		addScheduledTrainings(
								ctx.insertInto(TRAININGS, TRAININGS.TRNG_DATE, TRAININGS.TRNG_START, TRAININGS.TRNG_TRTY_FK, TRAININGS.TRNG_OUTCOME),
								10)
										.execute();

		// Adding trainers - 1% of employees are trainers
		ctx.insertInto(TRAININGS_TRAINERS, TRAININGS_TRAINERS.TRTR_TRNG_FK, TRAININGS_TRAINERS.TRTR_EMPL_FK)
				.select(DSL.select(DSL.field("trng_pk", Integer.class), DSL.field("empl_pk", String.class))
						.from(
								DSL.select(
											TRAININGS.TRNG_PK,
											DSL.floor(DSL.rand().mul(DSL.select(DSL.count()).from(EMPLOYEES).asField()).div(DSL.val(100))).as("linked_empl_id"))
										.from(TRAININGS).asTable("trainings2"))
						.join(DSL.select(
											EMPLOYEES.EMPL_PK,
											DSL.rowNumber().over().orderBy(DSL.rand()).as("empl_id"))
								.from(EMPLOYEES).asTable("employees2"))
						.on(DSL.field("linked_empl_id").eq(DSL.field("empl_id"))))
				.execute();

		// Adding VALIDATED employees - 1/4 or them, five times
		for (int i = 0; i < 4; i++) {
			ctx.insertInto(TRAININGS_EMPLOYEES, TRAININGS_EMPLOYEES.TREM_TRNG_FK, TRAININGS_EMPLOYEES.TREM_EMPL_FK, TRAININGS_EMPLOYEES.TREM_OUTCOME)
					.select(DSL.select(DSL.field("trng_pk", Integer.class), DSL.field("empl_pk", String.class), DSL.val(Constants.EMPL_OUTCOME_VALIDATED))
							.from(
									DSL.select(
												EMPLOYEES.EMPL_PK,
												DSL.floor(DSL.rand().mul(DSL.select(DSL.count()).from(TRAININGS)
														.where(TRAININGS.TRNG_OUTCOME.eq(Constants.TRNG_OUTCOME_COMPLETED))
														.asField()).mul(DSL.val(4)))
														.as("linked_trng_id"))
											.from(EMPLOYEES).where(EMPLOYEES.EMPL_PK.ne(Constants.USER_ROOT)).asTable("employees2"))
							.join(DSL.select(
												TRAININGS.TRNG_PK,
												DSL.rowNumber().over().orderBy(DSL.rand()).as("trng_id"))
									.from(TRAININGS).where(TRAININGS.TRNG_OUTCOME.eq(Constants.TRNG_OUTCOME_COMPLETED)).asTable("trainings2"))
							.on(DSL.field("linked_trng_id").eq(DSL.field("trng_id"))))
					.execute();
		}

		// Adding FLUNKED employees - 1/4 of them
		final Table<?> comments = DSL.values(Arrays.asList(
															"En incapacité physique d'effectuer certains gestes techniques",
															"Absent(e)",
															"A quitté la formation avant la fin",
															"En congés",
															"En arrêt maladie",
															"A été muté(e)")
				.stream().map(DSL::row).toArray(Row1[]::new)).as("unused", "trem_comment");

		ctx.insertInto(
						TRAININGS_EMPLOYEES,
						TRAININGS_EMPLOYEES.TREM_TRNG_FK,
						TRAININGS_EMPLOYEES.TREM_EMPL_FK,
						TRAININGS_EMPLOYEES.TREM_COMMENT,
						TRAININGS_EMPLOYEES.TREM_OUTCOME)
				.select(
						DSL.select(
									DSL.field("trng_pk", Integer.class),
									DSL.field("empl_pk", String.class),
									DSL.field("trem_comment", String.class),
									DSL.val(Constants.EMPL_OUTCOME_FLUNKED))
								.from(
										DSL.select(
													EMPLOYEES.EMPL_PK,
													DSL.floor(DSL.rand().mul(DSL.select(DSL.count()).from(TRAININGS)
															.where(TRAININGS.TRNG_OUTCOME.eq(Constants.TRNG_OUTCOME_COMPLETED))
															.asField()).mul(DSL.val(4))).as("linked_trng_id"),
													DSL.ceil(DSL.rand().mul(DSL.select(DSL.count()).from(comments).asField())).as("linked_trem_comment_id"))
												.from(EMPLOYEES).where(EMPLOYEES.EMPL_PK.ne(Constants.USER_ROOT)).asTable("employees2"))
								.join(DSL.select(
													TRAININGS.TRNG_PK,
													DSL.rowNumber().over().orderBy(DSL.rand()).as("trng_id"))
										.from(TRAININGS).where(TRAININGS.TRNG_OUTCOME.eq(Constants.TRNG_OUTCOME_COMPLETED)).asTable("trainings2"))
								.on(DSL.field("linked_trng_id").eq(DSL.field("trng_id")))
								.join(DSL.select(DSL.field("trem_comment"), DSL.rowNumber().over().as("trem_comment_id")).from(comments))
								.on(DSL.field("linked_trem_comment_id").eq(DSL.field("trem_comment_id"))))
				.execute();

		// Adding PENDING employees - 1/35 of them
		ctx.insertInto(TRAININGS_EMPLOYEES, TRAININGS_EMPLOYEES.TREM_TRNG_FK, TRAININGS_EMPLOYEES.TREM_EMPL_FK, TRAININGS_EMPLOYEES.TREM_OUTCOME)
				.select(DSL.select(DSL.field("trng_pk", Integer.class), DSL.field("empl_pk", String.class), DSL.val(Constants.EMPL_OUTCOME_PENDING))
						.from(
								DSL.select(
											EMPLOYEES.EMPL_PK,
											DSL.floor(DSL.rand().mul(DSL.select(DSL.count())
													.from(TRAININGS).where(TRAININGS.TRNG_OUTCOME.eq(Constants.TRNG_OUTCOME_SCHEDULED))
													.asField()).mul(DSL.val(35)))
													.as("linked_trng_id"))
										.from(EMPLOYEES).where(EMPLOYEES.EMPL_PK.ne(Constants.USER_ROOT)).asTable("employees2"))
						.join(DSL.select(
											TRAININGS.TRNG_PK,
											DSL.rowNumber().over().orderBy(DSL.rand()).as("trng_id"))
								.from(TRAININGS).where(TRAININGS.TRNG_OUTCOME.eq(Constants.TRNG_OUTCOME_SCHEDULED)).asTable("trainings2"))
						.on(DSL.field("linked_trng_id").eq(DSL.field("trng_id"))))
				.execute();
	}

	@SuppressWarnings("unchecked")
	private static Insert<?> addCompletedTrainings(final Insert<?> query, final int i) {
		final DateTime date = FAIRY.dateProducer().randomDateInThePast(4);
		return ((InsertValuesStep4<TrainingsRecord, Date, Date, Integer, String>) (i == 1 ? query : addCompletedTrainings(query, i - 1)))
				.values(asFields(
									new Date(date.getMillis()),
									new Date(date.minusDays(1).getMillis()),
									random(TRAININGTYPES, TRAININGTYPES.TRTY_PK),
									Constants.TRNG_OUTCOME_COMPLETED));
	}

	@SuppressWarnings("unchecked")
	private static Insert<?> addScheduledTrainings(final Insert<?> query, final int i) {
		final DateTime date = FAIRY.dateProducer().randomDateInTheFuture(1);
		return ((InsertValuesStep4<TrainingsRecord, Date, Date, Integer, String>) (i == 1 ? query : addScheduledTrainings(query, i - 1)))
				.values(asFields(
									new Date(date.getMillis()),
									new Date(date.minusDays(1).getMillis()),
									random(TRAININGTYPES, TRAININGTYPES.TRTY_PK),
									Constants.TRNG_OUTCOME_SCHEDULED));
	}

	public static <R> Field<R> random(final Table<?> table, final Field<R> field, final Condition... conditions) {
		return DSL.select(field).from(table).where(conditions).orderBy(DSL.rand()).limit(1).asField();
	}

	public static List<? extends Field<?>> asFields(final Object... values) {
		return Arrays.asList(values).stream().map(v -> v instanceof Field<?> ? (Field<?>) v : DSL.val(v)).collect(Collectors.toList());
	}
}
