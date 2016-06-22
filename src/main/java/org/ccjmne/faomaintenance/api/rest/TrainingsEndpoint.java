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
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import org.ccjmne.faomaintenance.api.utils.SafeDateFormat;
import org.ccjmne.faomaintenance.jooq.classes.Sequences;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

@Path("trainings")
public class TrainingsEndpoint {

	private final DSLContext ctx;
	private final ResourcesEndpoint resources;
	private final StatisticsEndpoint statistics;

	@Inject
	public TrainingsEndpoint(final DSLContext ctx, final ResourcesEndpoint resources, final StatisticsEndpoint statistics) {
		this.ctx = ctx;
		this.resources = resources;
		this.statistics = statistics;
	}

	@POST
	public Integer addTraining(final Map<String, Object> training) throws ParseException {
		return insertTraining(new Integer(this.ctx.nextval(Sequences.TRAININGS_TRNG_PK_SEQ).intValue()), training, this.ctx);
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
				final boolean exists = deleteTrainingImpl(trng_pk, transactionCtx);
				insertTraining(trng_pk, training, transactionCtx);
				return Boolean.valueOf(exists);
			}
		});
	}

	@DELETE
	@Path("{trng_pk}")
	public boolean deleteTraining(@PathParam("trng_pk") final Integer trng_pk) throws ParseException {
		return deleteTrainingImpl(trng_pk, this.ctx);
	}

	private boolean deleteTrainingImpl(final Integer trng_pk, final DSLContext transactionCtx) throws ParseException {
		final boolean exists = transactionCtx.selectFrom(TRAININGS).where(TRAININGS.TRNG_PK.equal(trng_pk)).fetch().isNotEmpty();
		if (exists) {
			transactionCtx.delete(TRAININGS_TRAINERS).where(TRAININGS_TRAINERS.TRTR_TRNG_FK.eq(trng_pk)).execute();
			this.statistics.invalidateEmployeesStats(this.resources.listEmployees(null, null, String.valueOf(trng_pk.intValue())).getValues(EMPLOYEES.EMPL_PK));
			transactionCtx.delete(TRAININGS).where(TRAININGS.TRNG_PK.equal(trng_pk)).execute();
		}

		return exists;
	}

	@SuppressWarnings("unchecked")
	private Integer insertTraining(final Integer trng_pk, final Map<String, Object> map, final DSLContext transactionContext) throws ParseException {
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
						(Integer) map.get("trng_trty_fk"),
						map.get("trng_start") != null ? SafeDateFormat.parseAsSql(map.get("trng_start").toString()) : null,
						SafeDateFormat.parseAsSql(map.get("trng_date").toString()),
						(String) map.get("trng_outcome"),
						(String) map.get("trng_comment"))
				.execute();
		final Map<String, Map<String, String>> trainees = (Map<String, Map<String, String>>) map.getOrDefault("trainees", Collections.EMPTY_MAP);
		this.statistics.invalidateEmployeesStats(trainees.keySet());
		trainees.forEach((trem_empl_fk, data) -> transactionContext.insertInto(
																				TRAININGS_EMPLOYEES,
																				TRAININGS_EMPLOYEES.TREM_TRNG_FK,
																				TRAININGS_EMPLOYEES.TREM_EMPL_FK,
																				TRAININGS_EMPLOYEES.TREM_OUTCOME,
																				TRAININGS_EMPLOYEES.TREM_COMMENT)
				.values(trng_pk, trem_empl_fk, data.get("trem_outcome"), data.get("trem_comment")).execute());
		((List<String>) map.getOrDefault("trainers", Collections.EMPTY_LIST))
				.forEach(trainer -> transactionContext.insertInto(
																	TRAININGS_TRAINERS,
																	TRAININGS_TRAINERS.TRTR_TRNG_FK,
																	TRAININGS_TRAINERS.TRTR_EMPL_FK)
						.values(trng_pk, trainer).execute());
		return trng_pk;
	}
}
