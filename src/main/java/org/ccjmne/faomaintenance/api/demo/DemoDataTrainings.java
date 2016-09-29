package org.ccjmne.faomaintenance.api.demo;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS_EMPLOYEES;
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
import org.jooq.Table;
import org.jooq.impl.DSL;

import io.codearte.jfairy.Fairy;

public class DemoDataTrainings {

	public static final Fairy FAIRY = Fairy.create(Locale.FRENCH);

	public static void generate(final DSLContext ctx) {
		addCompletedTrainings(
								ctx.insertInto(TRAININGS, TRAININGS.TRNG_DATE, TRAININGS.TRNG_START, TRAININGS.TRNG_TRTY_FK, TRAININGS.TRNG_OUTCOME),
								500)
										.execute();
		addScheduledTrainings(
								ctx.insertInto(TRAININGS, TRAININGS.TRNG_DATE, TRAININGS.TRNG_START, TRAININGS.TRNG_TRTY_FK, TRAININGS.TRNG_OUTCOME),
								10)
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
		ctx.insertInto(TRAININGS_EMPLOYEES, TRAININGS_EMPLOYEES.TREM_TRNG_FK, TRAININGS_EMPLOYEES.TREM_EMPL_FK, TRAININGS_EMPLOYEES.TREM_OUTCOME)
				.select(DSL.select(DSL.field("trng_pk", Integer.class), DSL.field("empl_pk", String.class), DSL.val(Constants.EMPL_OUTCOME_FLUNKED))
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

	private static <R> Field<R> random(final Table<?> table, final Field<R> field, final Condition... conditions) {
		return DSL.select(field).from(table).where(conditions).orderBy(DSL.rand()).limit(1).asField();
	}

	@SuppressWarnings("unchecked")
	private static List<? extends Field<?>> asFields(final Object... values) {
		return (List<? extends Field<?>>) Arrays.asList(values).stream().map(v -> v instanceof Field<?> ? v : DSL.val(v)).collect(Collectors.toList());
	}
}
