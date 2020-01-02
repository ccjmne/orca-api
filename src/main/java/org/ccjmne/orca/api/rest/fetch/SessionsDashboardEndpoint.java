package org.ccjmne.orca.api.rest.fetch;

import static org.ccjmne.orca.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS_TRAINERS;

import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import org.ccjmne.orca.api.modules.Restrictions;
import org.jooq.DSLContext;
import org.jooq.DatePart;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;

@Path("sessions-dashboard")
public class SessionsDashboardEndpoint {

  private final DSLContext ctx;

  @Inject()
  public SessionsDashboardEndpoint(final DSLContext ctx, final Restrictions restrictions) {
    if (!restrictions.canAccessTrainings()) {
      throw new ForbiddenException();
    }

    this.ctx = ctx;
  }

  @GET
  @Path("{year}/records")
  public Result<? extends Record> getRecords(@PathParam("year") final Integer year) {
    return this.ctx.select(
                           TRAININGS.TRNG_PK,
                           TRAININGS.TRNG_OUTCOME,
                           TRAININGS.TRNG_TRTY_FK,
                           DSL.trunc(TRAININGS.TRNG_DATE, DatePart.MONTH).as("month"),
                           TRAININGS_EMPLOYEES.TREM_OUTCOME,
                           EMPLOYEES.EMPL_GENDER,
                           EMPLOYEES.EMPL_PERMANENT,
                           DSL.coalesce(DSL.arrayAgg(TRAININGS_TRAINERS.TRTR_EMPL_FK).filterWhere(TRAININGS_TRAINERS.TRTR_EMPL_FK.isNotNull()), DSL.array())
                               .as("instructors"))
        .from(TRAININGS_EMPLOYEES
            .join(TRAININGS).on(TRAININGS.TRNG_PK.eq(TRAININGS_EMPLOYEES.TREM_TRNG_FK))
            .join(EMPLOYEES).on(EMPLOYEES.EMPL_PK.eq(TRAININGS_EMPLOYEES.TREM_EMPL_FK))
            .leftOuterJoin(TRAININGS_TRAINERS).on(TRAININGS_TRAINERS.TRTR_TRNG_FK.eq(TRAININGS.TRNG_PK)))
        .where(DSL.extract(TRAININGS.TRNG_DATE, DatePart.YEAR).eq(year))
        .groupBy(
                 TRAININGS_EMPLOYEES.TREM_PK,
                 TRAININGS.TRNG_PK,
                 TRAININGS.TRNG_OUTCOME,
                 TRAININGS.TRNG_TRTY_FK,
                 DSL.trunc(TRAININGS.TRNG_DATE, DatePart.MONTH).as("month"),
                 TRAININGS_EMPLOYEES.TREM_OUTCOME,
                 EMPLOYEES.EMPL_GENDER,
                 EMPLOYEES.EMPL_PERMANENT)
        .fetch();
  }

  @GET
  @Path("{year}/instructors")
  public Result<? extends Record> getInstructors(@PathParam("year") final Integer year) {
    return this.ctx.select(EMPLOYEES.fields())
        .from(EMPLOYEES)
        .where(EMPLOYEES.EMPL_PK.in(DSL
            .select(TRAININGS_TRAINERS.TRTR_EMPL_FK)
            .from(TRAININGS_TRAINERS)
            .join(TRAININGS).on(TRAININGS.TRNG_PK.eq(TRAININGS_TRAINERS.TRTR_TRNG_FK))
            .where(DSL.extract(TRAININGS.TRNG_DATE, DatePart.YEAR).eq(year))))
        .fetch();
  }
}
