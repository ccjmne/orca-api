package org.ccjmne.orca.api.demo;

import static org.ccjmne.orca.jooq.codegen.Tables.EMPLOYEES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGS;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGS_TRAINERS;

import java.time.LocalDate;
import java.util.Arrays;

import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.jooq.codegen.tables.records.TrainingsEmployeesRecord;
import org.ccjmne.orca.jooq.codegen.tables.records.TrainingsRecord;
import org.jooq.DSLContext;
import org.jooq.Insert;
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

    // Set trainers - 1% of employees are trainers
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

    // Add VALIDATED trainees - 1/4 of them, five times
    for (int i = 0; i < 5; i++) {
      ctx.execute(DemoDataTrainings.addTrainees(4, Constants.TRNG_OUTCOME_COMPLETED, Constants.EMPL_OUTCOME_VALIDATED, ""));
    }

    // Add FLUNKED trainees - 1/4 of them
    ctx.execute(DemoDataTrainings.addTrainees(4, Constants.TRNG_OUTCOME_COMPLETED, Constants.EMPL_OUTCOME_FLUNKED,
                                              "En incapacité physique d'effectuer certains gestes techniques", "A quitté la formation avant la fin"));

    // Add MISSING trainees - 1/4 of them
    ctx.execute(DemoDataTrainings.addTrainees(4, Constants.TRNG_OUTCOME_COMPLETED, Constants.EMPL_OUTCOME_MISSING,
                                              "Absent(e)", "En congés", "A été muté(e)"));

    // Add PENDING trainees - 1/35 of them
    ctx.execute(DemoDataTrainings.addTrainees(35, Constants.TRNG_OUTCOME_SCHEDULED, Constants.EMPL_OUTCOME_PENDING, ""));

    // Add CANCELLED trainees - 1/7 of them
    ctx.execute(DemoDataTrainings.addTrainees(7, Constants.TRNG_OUTCOME_CANCELLED, Constants.EMPL_OUTCOME_CANCELLED, ""));
  }

  @SuppressWarnings("unchecked")
  private static Insert<TrainingsRecord> addTrainings(final String outcome, final int amount) {
    final Table<Record4<Integer, LocalDate, String, String>> sessions = new FakeRecords().sessions(outcome, amount);
    return DSL.insertInto(TRAININGS, TRAININGS.TRNG_TRTY_FK, TRAININGS.TRNG_DATE, TRAININGS.TRNG_OUTCOME, TRAININGS.TRNG_COMMENT)
        .select((Select<? extends Record4<Integer, LocalDate, String, String>>) DSL.select(sessions.fields()).from(sessions));
  }

  @SuppressWarnings("unchecked")
  public static Insert<TrainingsEmployeesRecord> addTrainees(
                                                             final int oneIn,
                                                             final String sessionOutcome,
                                                             final String traineeOutcome,
                                                             final String... comments) {
    return DSL.insertInto(
                          TRAININGS_EMPLOYEES,
                          TRAININGS_EMPLOYEES.TREM_TRNG_FK,
                          TRAININGS_EMPLOYEES.TREM_EMPL_FK,
                          TRAININGS_EMPLOYEES.TREM_COMMENT,
                          TRAININGS_EMPLOYEES.TREM_OUTCOME)
        .select(DSL.select(
                           DSL.field("trng_pk", Integer.class),
                           DSL.field("empl_pk", Integer.class),
                           DSL.field("trem_comment", String.class),
                           DSL.val(traineeOutcome))
            .from(DSL.select(
                             EMPLOYEES.EMPL_PK,
                             DSL.floor(DSL.rand().mul(DSL.select(DSL.count()).from(TRAININGS)
                                 .where(TRAININGS.TRNG_OUTCOME.eq(sessionOutcome))
                                 .asField()).mul(DSL.val(oneIn))).as("linked_trng_id"),
                             DSL.ceil(DSL.rand().mul(Integer.valueOf(comments.length))).as("linked_trem_comment_id"))
                .from(EMPLOYEES).where(EMPLOYEES.EMPL_PK.ne(Constants.EMPLOYEE_ROOT)).asTable("employees_view"))
            .join(DSL.select(
                             TRAININGS.TRNG_PK,
                             DSL.rowNumber().over().orderBy(DSL.rand()).as("trng_id"))
                .from(TRAININGS).where(TRAININGS.TRNG_OUTCOME.eq(sessionOutcome)).asTable("trainings_view"))
            .on(DSL.field("linked_trng_id").eq(DSL.field("trng_id")))
            .join(DSL.select(
                             DSL.field("trem_comment"),
                             DSL.rowNumber().over().as("trem_comment_id"))
                .from(DSL.values(Arrays.stream(comments).map(DSL::row).toArray(Row1[]::new)).as("unused", "trem_comment")))
            .on(DSL.field("linked_trem_comment_id").eq(DSL.field("trem_comment_id"))))
        .onDuplicateKeyIgnore();
  }
}
