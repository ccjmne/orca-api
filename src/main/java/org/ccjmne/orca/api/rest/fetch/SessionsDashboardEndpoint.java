package org.ccjmne.orca.api.rest.fetch;

import static org.ccjmne.orca.jooq.codegen.Tables.EMPLOYEES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGS;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGS_TRAINERS;

import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import org.ccjmne.orca.api.modules.Restrictions;
import org.ccjmne.orca.api.utils.Constants;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.DatePart;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;

@Path("sessions-dashboard")
public class SessionsDashboardEndpoint {

  private static final Field<Integer> FIELD_YEAR  = DSL.extract(TRAININGS.TRNG_DATE, DatePart.YEAR).as("year");
  private static final Field<Integer> FIELD_MONTH = DSL.extract(TRAININGS.TRNG_DATE, DatePart.MONTH).as("month");
  private static final Condition      IS_RELEVANT = TRAININGS.TRNG_OUTCOME.eq(Constants.TRNG_OUTCOME_COMPLETED);

  private final DSLContext ctx;

  @Inject()
  public SessionsDashboardEndpoint(final DSLContext ctx, final Restrictions restrictions) {
    if (!restrictions.canAccessTrainings()) {
      throw new ForbiddenException();
    }

    this.ctx = ctx;
  }

  /**
   * The special value {@code -1} for instructor ID denotes the absence of any
   * assigned instructor.<br />
   * This may occur when the training was provided by an external entity.
   *
   * @return The records for all trainees for a given {@code year}.
   */
  @GET
  @Path("records")
  public Result<? extends Record> getAllRecords() {
    return this.ctx.select(
                           TRAININGS.TRNG_PK,
                           TRAININGS.TRNG_OUTCOME,
                           TRAININGS.TRNG_TRTY_FK,
                           SessionsDashboardEndpoint.FIELD_YEAR,
                           SessionsDashboardEndpoint.FIELD_MONTH,
                           TRAININGS_EMPLOYEES.TREM_OUTCOME,
                           EMPLOYEES.EMPL_GENDER,
                           EMPLOYEES.EMPL_PERMANENT,
                           DSL.coalesce(DSL.arrayAgg(TRAININGS_TRAINERS.TRTR_EMPL_FK).filterWhere(TRAININGS_TRAINERS.TRTR_EMPL_FK.isNotNull()),
                                        DSL.array(Integer.valueOf(-1)))
                               .as("instructors"))
        .from(TRAININGS_EMPLOYEES
            .join(TRAININGS).on(TRAININGS.TRNG_PK.eq(TRAININGS_EMPLOYEES.TREM_TRNG_FK))
            .join(EMPLOYEES).on(EMPLOYEES.EMPL_PK.eq(TRAININGS_EMPLOYEES.TREM_EMPL_FK))
            .leftOuterJoin(TRAININGS_TRAINERS).on(TRAININGS_TRAINERS.TRTR_TRNG_FK.eq(TRAININGS.TRNG_PK)))
        .where(SessionsDashboardEndpoint.IS_RELEVANT)
        .groupBy(
                 TRAININGS_EMPLOYEES.TREM_PK,
                 TRAININGS.TRNG_PK,
                 TRAININGS.TRNG_OUTCOME,
                 TRAININGS.TRNG_TRTY_FK,
                 SessionsDashboardEndpoint.FIELD_MONTH,
                 TRAININGS_EMPLOYEES.TREM_OUTCOME,
                 EMPLOYEES.EMPL_GENDER,
                 EMPLOYEES.EMPL_PERMANENT)
        .fetch();
  }

  /**
   * Fetches the identity of each instructor that was assigned to a training
   * session for a given year.
   *
   * @return A {@code Map} of instructors (effectively: employees) keyed by their
   *         ID
   */
  @GET
  @Path("instructors")
  public Map<Integer, ? extends Record> getAllInstructors() {
    return this.ctx.select(EMPLOYEES.fields())
        .from(EMPLOYEES)
        .where(EMPLOYEES.EMPL_PK.in(DSL
            .select(TRAININGS_TRAINERS.TRTR_EMPL_FK)
            .from(TRAININGS_TRAINERS)
            .join(TRAININGS).on(TRAININGS.TRNG_PK.eq(TRAININGS_TRAINERS.TRTR_TRNG_FK))
            .where(SessionsDashboardEndpoint.IS_RELEVANT)))
        .fetchMap(EMPLOYEES.EMPL_PK);
  }
}
