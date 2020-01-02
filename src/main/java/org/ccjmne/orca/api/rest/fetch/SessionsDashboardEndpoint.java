package org.ccjmne.orca.api.rest.fetch;

import static org.ccjmne.orca.jooq.codegen.Tables.EMPLOYEES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGS;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGS_TRAINERS;

import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import org.ccjmne.orca.api.inject.business.QueryParams;
import org.ccjmne.orca.api.inject.business.Restrictions;
import org.jooq.DSLContext;
import org.jooq.DatePart;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;

@Path("sessions-dashboard")
public class SessionsDashboardEndpoint {

  private final DSLContext ctx;

  private final Field<Integer> year;

  @Inject()
  public SessionsDashboardEndpoint(final DSLContext ctx, final Restrictions restrictions, final QueryParams parameters) {
    if (!restrictions.canAccessSessions()) {
      throw new ForbiddenException();
    }

    this.ctx = ctx;
    this.year = parameters.get(QueryParams.YEAR);
  }

  @GET
  @Path("{year}/records")
  public Result<? extends Record> getRecords() {
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
        .where(DSL.extract(TRAININGS.TRNG_DATE, DatePart.YEAR).eq(this.year))
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
  public Result<? extends Record> getInstructors() {
    return this.ctx.select(EMPLOYEES.fields())
        .from(EMPLOYEES)
        .where(EMPLOYEES.EMPL_PK.in(DSL
            .select(TRAININGS_TRAINERS.TRTR_EMPL_FK)
            .from(TRAININGS_TRAINERS)
            .join(TRAININGS).on(TRAININGS.TRNG_PK.eq(TRAININGS_TRAINERS.TRTR_TRNG_FK))
            .where(DSL.extract(TRAININGS.TRNG_DATE, DatePart.YEAR).eq(this.year))))
        .fetch();
  }
}
