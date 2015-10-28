package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS_EMPLOYEES;

import java.text.ParseException;
import java.util.Collections;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import org.ccjmne.faomaintenance.api.utils.SQLDateFormat;
import org.ccjmne.faomaintenance.jooq.classes.Sequences;
import org.jooq.DSLContext;

@Path("trainings")
public class TrainingsEndpoint {

	private final DSLContext ctx;
	private final SQLDateFormat dateFormat;
	private final ResourcesEndpoint resources;
	private final StatisticsEndpoint statistics;

	@Inject
	public TrainingsEndpoint(final DSLContext ctx, final SQLDateFormat dateFormat, final ResourcesEndpoint resources, final StatisticsEndpoint statistics) {
		this.ctx = ctx;
		this.dateFormat = dateFormat;
		this.resources = resources;
		this.statistics = statistics;
	}

	@POST
	public Integer addTraining(final Map<String, Object> training) throws ParseException {
		return insertTraining(new Integer(this.ctx.nextval(Sequences.TRAININGS_TRNG_PK_SEQ).intValue()), training);
	}

	@PUT
	@Path("{trng_pk}")
	public boolean updateTraining(@PathParam("trng_pk") final Integer trng_pk, final Map<String, Object> training) throws ParseException {
		final boolean exists = deleteTraining(trng_pk);
		insertTraining(trng_pk, training);
		return exists;
	}

	@DELETE
	@Path("{trng_pk}")
	public boolean deleteTraining(@PathParam("trng_pk") final Integer trng_pk) throws ParseException {
		final boolean exists = this.ctx.selectFrom(TRAININGS).where(TRAININGS.TRNG_PK.equal(trng_pk)).fetch().isNotEmpty();
		if (exists) {
			this.statistics.invalidateEmployeesStats(this.resources.listEmployees(null, null, String.valueOf(trng_pk.intValue())).getValues(EMPLOYEES.EMPL_PK));
			this.ctx.delete(TRAININGS).where(TRAININGS.TRNG_PK.equal(trng_pk)).execute();
		}

		return exists;
	}

	@SuppressWarnings("unchecked")
	private Integer insertTraining(final Integer trng_pk, final Map<String, Object> map) throws ParseException {
		this.ctx
				.insertInto(TRAININGS, TRAININGS.TRNG_PK, TRAININGS.TRNG_TRTY_FK, TRAININGS.TRNG_DATE, TRAININGS.TRNG_OUTCOME)
				.values(
						trng_pk,
						(Integer) map.get("trng_trty_fk"),
						this.dateFormat.parseSql(map.get("trng_date").toString()), map.get("trng_outcome").toString()).execute();
		final Map<String, Map<String, String>> trainees = (Map<String, Map<String, String>>) map.getOrDefault("trainees", Collections.EMPTY_MAP);
		this.statistics.invalidateEmployeesStats(trainees.keySet());
		trainees.forEach((trainee, info) ->
				this.ctx.insertInto(
									TRAININGS_EMPLOYEES,
									TRAININGS_EMPLOYEES.TREM_TRNG_FK,
									TRAININGS_EMPLOYEES.TREM_EMPL_FK,
									TRAININGS_EMPLOYEES.TREM_OUTCOME,
									TRAININGS_EMPLOYEES.TREM_COMMENT)
						.values(trng_pk, trainee, info.get("trem_outcome"), info.get("trem_comment")).execute());

		return trng_pk;
	}
}
