package org.ccjmne.orca.api.rest.edit;

import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS_TRAINERS;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGTYPES;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import org.ccjmne.orca.api.modules.Restrictions;
import org.ccjmne.orca.api.rest.fetch.ResourcesEndpoint;
import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.api.utils.SafeDateFormat;
import org.ccjmne.orca.jooq.classes.Sequences;
import org.ccjmne.orca.jooq.classes.tables.records.TrainingsRecord;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

@Path("trainings")
public class TrainingsEndpoint {

	private final DSLContext ctx;
	private final Restrictions restrictions;

	@Inject
	public TrainingsEndpoint(final DSLContext ctx, final ResourcesEndpoint resources, final Restrictions restrictions) {
		this.ctx = ctx;
		this.restrictions = restrictions;
	}

	@POST
	public Integer addTraining(final Map<String, Object> training) {
		return this.ctx.transactionResult(config -> {
			try (final DSLContext transactionContext = DSL.using(config)) {
				return this.insertTraining(new Integer(transactionContext.nextval(Sequences.TRAININGS_TRNG_PK_SEQ).intValue()), training, transactionContext);
			}
		});
	}

	@POST
	@Path("/bulk")
	public void addTrainings(final List<Map<String, Object>> trainings) {
		this.ctx.transaction(config -> {
			try (final DSLContext transactionContext = DSL.using(config)) {
				for (final Map<String, Object> training : trainings) {
					this.insertTraining(new Integer(transactionContext.nextval(Sequences.TRAININGS_TRNG_PK_SEQ).intValue()), training, transactionContext);
				}
			}
		});
	}

	@PUT
	@Path("{trng_pk}")
	public Boolean updateTraining(@PathParam("trng_pk") final Integer trng_pk, final Map<String, Object> training) {
		return this.ctx.transactionResult(config -> {
			try (final DSLContext transactionCtx = DSL.using(config)) {
				final Boolean exists = this.deleteTrainingImpl(trng_pk, transactionCtx);
				this.insertTraining(trng_pk, training, transactionCtx);
				return exists;
			}
		});
	}

	@DELETE
	@Path("{trng_pk}")
	public Boolean deleteTraining(@PathParam("trng_pk") final Integer trng_pk) {
		return this.ctx.transactionResult(config -> {
			try (final DSLContext transactionCtx = DSL.using(config)) {
				return this.deleteTrainingImpl(trng_pk, transactionCtx);
			}
		});
	}

	private Boolean deleteTrainingImpl(final Integer trng_pk, final DSLContext transactionCtx) {
		final TrainingsRecord training = transactionCtx.selectFrom(TRAININGS).where(TRAININGS.TRNG_PK.equal(trng_pk)).fetchOne();
		if (training != null) {
			if (!this.restrictions.getManageableTypes().contains(training.getTrngTrtyFk())) {
				throw new ForbiddenException();
			}

			transactionCtx.delete(TRAININGS_TRAINERS).where(TRAININGS_TRAINERS.TRTR_TRNG_FK.eq(trng_pk)).execute();
			transactionCtx.delete(TRAININGS).where(TRAININGS.TRNG_PK.equal(trng_pk)).execute();
			return Boolean.TRUE;
		}

		return Boolean.FALSE;
	}

	@SuppressWarnings("unchecked")
	private Integer insertTraining(final Integer trng_pk, final Map<String, Object> map, final DSLContext transactionContext) throws ParseException {
		if (!this.restrictions.getManageableTypes().contains(map.get(TRAININGS.TRNG_TRTY_FK.getName()))) {
			throw new ForbiddenException();
		}

		TrainingsEndpoint.validateOutcomes(map, transactionContext.selectFrom(TRAININGTYPES)
				.where(TRAININGTYPES.TRTY_PK.eq(Integer.valueOf(map.get(TRAININGTYPES.TRTY_PK.getName()).toString())))
				.fetchOne(TRAININGTYPES.TRTY_PRESENCEONLY).booleanValue());

		transactionContext
				.insertInto(
							TRAININGS,
							TRAININGS.TRNG_PK,
							TRAININGS.TRNG_TRTY_FK,
							TRAININGS.TRNG_START,
							TRAININGS.TRNG_DATE,
							TRAININGS.TRNG_OUTCOME,
							TRAININGS.TRNG_COMMENT)
				.values(
						trng_pk,
						(Integer) map.get(TRAININGS.TRNG_TRTY_FK.getName()),
						map.get(TRAININGS.TRNG_START.getName()) != null ? SafeDateFormat.parseAsSql(map.get(TRAININGS.TRNG_START.getName()).toString()) : null,
						SafeDateFormat.parseAsSql(map.get(TRAININGS.TRNG_DATE.getName()).toString()),
						(String) map.get(TRAININGS.TRNG_OUTCOME.getName()),
						(String) map.get(TRAININGS.TRNG_COMMENT.getName()))
				.execute();

		((Map<String, Map<String, String>>) map.getOrDefault("trainees", Collections.emptyMap()))
				.forEach((trem_empl_fk, data) -> transactionContext
						.insertInto(
									TRAININGS_EMPLOYEES,
									TRAININGS_EMPLOYEES.TREM_TRNG_FK,
									TRAININGS_EMPLOYEES.TREM_EMPL_FK,
									TRAININGS_EMPLOYEES.TREM_OUTCOME,
									TRAININGS_EMPLOYEES.TREM_COMMENT)
						.values(
								trng_pk,
								Integer.valueOf(trem_empl_fk),
								data.get(TRAININGS_EMPLOYEES.TREM_OUTCOME.getName()),
								data.get(TRAININGS_EMPLOYEES.TREM_COMMENT.getName()))
						.execute());

		((List<Integer>) map.getOrDefault("trainers", Collections.EMPTY_LIST))
				.forEach(trainer -> transactionContext
						.insertInto(TRAININGS_TRAINERS, TRAININGS_TRAINERS.TRTR_TRNG_FK, TRAININGS_TRAINERS.TRTR_EMPL_FK)
						.values(trng_pk, trainer).execute());
		return trng_pk;
	}

	@SuppressWarnings("unchecked")
	private static void validateOutcomes(final Map<String, Object> training, final boolean presenceOnly) {
		if (!Constants.TRAINING_OUTCOMES.contains(training.get(TRAININGS.TRNG_OUTCOME.getName()))) {
			throw new IllegalArgumentException(String.format("The outcome of a training must be one of %s.", Constants.TRAINING_OUTCOMES));
		}

		if (!training.containsKey("trainees")) {
			return;
		}

		final Predicate<String> predicate;
		switch ((String) training.get(TRAININGS.TRNG_OUTCOME.getName())) {
			case Constants.TRNG_OUTCOME_CANCELLED:
				predicate = outcome -> outcome.equals(Constants.EMPL_OUTCOME_CANCELLED);
				break;
			case Constants.TRNG_OUTCOME_COMPLETED:
				predicate = outcome -> (presenceOnly
														? Arrays.asList(Constants.EMPL_OUTCOME_VALIDATED, Constants.EMPL_OUTCOME_MISSING)
														: Arrays.asList(Constants.EMPL_OUTCOME_VALIDATED, Constants.EMPL_OUTCOME_MISSING,
																		Constants.EMPL_OUTCOME_FLUNKED))
																				.contains(outcome);
				break;
			default: // TRNG_OUTCOME_SCHEDULED
				predicate = outcome -> outcome.equals(Constants.EMPL_OUTCOME_PENDING);
		}

		if (!((Map<String, Map<String, String>>) training.get("trainees")).values().stream()
				.map(trainee -> trainee.get(TRAININGS_EMPLOYEES.TREM_OUTCOME.getName())).allMatch(predicate)) {
			throw new IllegalArgumentException("Some trainees' outcomes are incompatible with the training session's outcome.");
		}
	}
}
