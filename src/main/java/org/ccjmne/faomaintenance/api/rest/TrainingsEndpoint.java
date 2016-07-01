package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS_TRAINERS;

import java.text.ParseException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import org.ccjmne.faomaintenance.api.modules.Restrictions;
import org.ccjmne.faomaintenance.api.modules.StatisticsCaches;
import org.ccjmne.faomaintenance.api.utils.SafeDateFormat;
import org.ccjmne.faomaintenance.jooq.classes.Sequences;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.TrainingsRecord;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

@Path("trainings")
public class TrainingsEndpoint {

	private final DSLContext ctx;
	private final ResourcesEndpoint resources;
	private final StatisticsCaches statistics;
	private final Restrictions restrictions;

	@Inject
	public TrainingsEndpoint(final DSLContext ctx, final ResourcesEndpoint resources, final StatisticsCaches statistics, final Restrictions restrictions) {
		this.ctx = ctx;
		this.resources = resources;
		this.statistics = statistics;
		this.restrictions = restrictions;
	}

	@POST
	public Integer addTraining(final Map<String, Object> training) {
		return this.ctx.transactionResult(config -> {
			try (final DSLContext transactionContext = DSL.using(config)) {
				return insertTraining(new Integer(transactionContext.nextval(Sequences.TRAININGS_TRNG_PK_SEQ).intValue()), training, transactionContext);
			}
		});
	}

	@POST
	@Path("/bulk")
	public void addTrainings(final List<Map<String, Object>> trainings) {
		this.ctx.transaction(config -> {
			try (final DSLContext transactionContext = DSL.using(config)) {
				for (final Map<String, Object> training : trainings) {
					insertTraining(new Integer(transactionContext.nextval(Sequences.TRAININGS_TRNG_PK_SEQ).intValue()), training, transactionContext);
				}
			}
		});
	}

	@PUT
	@Path("{trng_pk}")
	public Boolean updateTraining(@PathParam("trng_pk") final Integer trng_pk, final Map<String, Object> training) {
		return this.ctx.transactionResult(config -> {
			try (final DSLContext transactionCtx = DSL.using(config)) {
				final Boolean exists = deleteTrainingImpl(trng_pk, transactionCtx);
				insertTraining(trng_pk, training, transactionCtx);
				return exists;
			}
		});
	}

	@DELETE
	@Path("{trng_pk}")
	public Boolean deleteTraining(@PathParam("trng_pk") final Integer trng_pk) {
		return this.ctx.transactionResult(config -> {
			try (final DSLContext transactionCtx = DSL.using(config)) {
				return deleteTrainingImpl(trng_pk, transactionCtx);
			}
		});
	}

	private Boolean deleteTrainingImpl(final Integer trng_pk, final DSLContext transactionCtx) throws ParseException {
		final TrainingsRecord training = transactionCtx.selectFrom(TRAININGS).where(TRAININGS.TRNG_PK.equal(trng_pk)).fetchOne();
		if (training != null) {
			if (!this.restrictions.getManageableTypes().contains(training.getTrngTrtyFk())) {
				throw new ForbiddenException();
			}

			transactionCtx.delete(TRAININGS_TRAINERS).where(TRAININGS_TRAINERS.TRTR_TRNG_FK.eq(trng_pk)).execute();
			this.statistics.invalidateEmployeesStats(this.resources.listEmployees(null, null, String.valueOf(trng_pk.intValue())).getValues(EMPLOYEES.EMPL_PK));
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
		final Map<String, Map<String, String>> trainees = (Map<String, Map<String, String>>) map.getOrDefault("trainees", Collections.EMPTY_MAP);
		this.statistics.invalidateEmployeesStats(trainees.keySet());
		trainees.forEach((trem_empl_fk, data) -> transactionContext
				.insertInto(
							TRAININGS_EMPLOYEES,
							TRAININGS_EMPLOYEES.TREM_TRNG_FK,
							TRAININGS_EMPLOYEES.TREM_EMPL_FK,
							TRAININGS_EMPLOYEES.TREM_OUTCOME,
							TRAININGS_EMPLOYEES.TREM_COMMENT)
				.values(
						trng_pk,
						trem_empl_fk,
						data.get(TRAININGS_EMPLOYEES.TREM_OUTCOME.getName()),
						data.get(TRAININGS_EMPLOYEES.TREM_COMMENT.getName()))
				.execute());
		((List<String>) map.getOrDefault("trainers", Collections.EMPTY_LIST))
				.forEach(trainer -> transactionContext.insertInto(
																	TRAININGS_TRAINERS,
																	TRAININGS_TRAINERS.TRTR_TRNG_FK,
																	TRAININGS_TRAINERS.TRTR_EMPL_FK)
						.values(trng_pk, trainer).execute());
		return trng_pk;
	}
}
