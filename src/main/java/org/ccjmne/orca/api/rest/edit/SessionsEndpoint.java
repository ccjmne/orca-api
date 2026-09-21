package org.ccjmne.orca.api.rest.edit;

import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGS;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGS_TRAINERS;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGTYPES_DEFS;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

import org.ccjmne.orca.api.inject.business.QueryParams;
import org.ccjmne.orca.api.inject.business.Restrictions;
import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.api.utils.Fields;
import org.ccjmne.orca.api.utils.Transactions;
import org.ccjmne.orca.jooq.codegen.tables.records.TrainingtypesDefsRecord;
import org.ccjmne.orca.jooq.codegen.tables.records.TrainingsRecord;
import org.jooq.DSLContext;
import org.jooq.Param;
import org.jooq.Record1;
import org.jooq.Row1;
import org.jooq.Table;
import org.jooq.impl.DSL;

import com.fasterxml.jackson.databind.node.TextNode;

@Path("sessions")
public class SessionsEndpoint {

  private static enum Check {
                             IS_EXISTING,
                             IS_MANAGEABLE,
                             IS_SCHEDULED,
                             IS_NOT_CANCELLED,
                             IS_NOT_COMPLETED
  }

  private static final List<String> COMPLETED_OUTCOMES = Arrays
      .asList(Constants.EMPL_OUTCOME_VALIDATED, Constants.EMPL_OUTCOME_FLUNKED, Constants.EMPL_OUTCOME_MISSING);
  private static final List<String> PRESENCE_ONLY_COMPLETED_OUTCOMES = Arrays
      .asList(Constants.EMPL_OUTCOME_VALIDATED, Constants.EMPL_OUTCOME_MISSING);

  private final DSLContext     ctx;
  private final Restrictions   restrictions;
  private final Param<Integer> sessionId;

  @Inject
  public SessionsEndpoint(final DSLContext ctx, final Restrictions restrictions, final QueryParams parameters) {
    this.ctx = ctx;
    this.restrictions = restrictions;
    this.sessionId = parameters.get(QueryParams.SESSION);
  }

  /**
   * Create a new session, as described by the request's JSON body.
   *
   * @param session
   *          A JSON object representing the new session to create.
   * @return The identifier of the newly created session.
   */
  @POST
  @SuppressWarnings("unchecked")
  public Integer createSession(final Map<String, Object> session) {
    return Transactions.with(this.ctx, transactionCtx -> {
      final Integer type = (Integer) session.get(TRAININGS.TRNG_TRTY_FK.getName());
      final LocalDate date = LocalDate.parse((String) session.get(TRAININGS.TRNG_DATE.getName()));
      if (!this.restrictions.getManageableTypes().contains(type)) {
        throw new ForbiddenException("This user account is not allowed to create sessions of that type.");
      }
      SessionsEndpoint.effectiveDefinition(transactionCtx, type, date);

      final Integer id = transactionCtx
          .insertInto(
                      TRAININGS,
                      TRAININGS.TRNG_TRTY_FK,
                      TRAININGS.TRNG_START,
                      TRAININGS.TRNG_DATE,
                      TRAININGS.TRNG_OUTCOME,
                      TRAININGS.TRNG_COMMENT)
          .values(
                  type,
                  session.get(TRAININGS.TRNG_START.getName()) == null ? null : LocalDate.parse((String) session.get(TRAININGS.TRNG_START.getName())),
                  date,
                  Constants.TRNG_OUTCOME_SCHEDULED,
                  (String) session.get(TRAININGS.TRNG_COMMENT.getName()))
          .returningResult(TRAININGS.TRNG_PK)
          .fetchOne().value1();

      ((List<Integer>) session.getOrDefault("trainers", Collections.EMPTY_LIST))
          .forEach(instructor -> transactionCtx
              .insertInto(TRAININGS_TRAINERS, TRAININGS_TRAINERS.TRTR_TRNG_FK, TRAININGS_TRAINERS.TRTR_EMPL_FK)
              .values(id, instructor)
              .execute());
      return id;
    });
  }

  /**
   * Update an existing session, as described by the request's JSON body.
   *
   * @param session
   *          A JSON object representing the session to update.
   */
  @PUT
  @Path("{session}")
  @SuppressWarnings("unchecked")
  public void updateSession(final Map<String, Object> session) {
    Transactions.with(this.ctx, transactionCtx -> {
      final TrainingsRecord existing = this.ensure(transactionCtx, Check.IS_EXISTING, Check.IS_MANAGEABLE, Check.IS_NOT_CANCELLED);
      final Integer type = (Integer) session.get(TRAININGS.TRNG_TRTY_FK.getName());
      final LocalDate date = LocalDate.parse((String) session.get(TRAININGS.TRNG_DATE.getName()));
      if (!this.restrictions.getManageableTypes().contains(type)) {
        throw new ForbiddenException("This user account is not allowed to manage sessions of that type.");
      }
      final TrainingtypesDefsRecord definition = SessionsEndpoint.effectiveDefinition(transactionCtx, type, date);
      SessionsEndpoint.validateOutcomes(
                                        definition,
                                        existing.getTrngOutcome(),
                                        transactionCtx.select(TRAININGS_EMPLOYEES.TREM_OUTCOME)
                                            .from(TRAININGS_EMPLOYEES)
                                            .where(TRAININGS_EMPLOYEES.TREM_TRNG_FK.eq(this.sessionId))
                                            .fetch(TRAININGS_EMPLOYEES.TREM_OUTCOME));

      transactionCtx
          .update(TRAININGS)
          .set(TRAININGS.TRNG_TRTY_FK, type)
          .set(TRAININGS.TRNG_START, session
              .get(TRAININGS.TRNG_START.getName()) == null ? null : LocalDate.parse((String) session.get(TRAININGS.TRNG_START.getName())))
          .set(TRAININGS.TRNG_DATE, date)
          .set(TRAININGS.TRNG_COMMENT, (String) session.get(TRAININGS.TRNG_COMMENT.getName()))
          .where(TRAININGS.TRNG_PK.eq(this.sessionId))
          .execute();

      transactionCtx.deleteFrom(TRAININGS_TRAINERS).where(TRAININGS_TRAINERS.TRTR_TRNG_FK.eq(this.sessionId)).execute();
      ((List<Integer>) session.getOrDefault("trainers", Collections.EMPTY_LIST))
          .forEach(instructor -> transactionCtx
              .insertInto(TRAININGS_TRAINERS, TRAININGS_TRAINERS.TRTR_TRNG_FK, TRAININGS_TRAINERS.TRTR_EMPL_FK)
              .values(this.sessionId.getValue(), instructor)
              .execute());
    });
  }

  @DELETE
  @Path("{session}")
  public void deleteSession() {
    Transactions.with(this.ctx, transactionCtx -> {
      this.ensure(transactionCtx, Check.IS_EXISTING, Check.IS_MANAGEABLE);

      transactionCtx.deleteFrom(TRAININGS).where(TRAININGS.TRNG_PK.eq(this.sessionId)).execute();
    });
  }

  @PUT
  @Path("{session}/notes")
  public void updateNotes(final TextNode notes) {
    Transactions.with(this.ctx, transactionCtx -> {
      this.ensure(transactionCtx, Check.IS_EXISTING, Check.IS_MANAGEABLE);

      transactionCtx.update(TRAININGS).set(TRAININGS.TRNG_COMMENT, notes.asText()).where(TRAININGS.TRNG_PK.eq(this.sessionId)).execute();
    });
  }

  @POST
  @Path("{session}/cancel")
  public void cancelSession() {
    Transactions.with(this.ctx, transactionCtx -> {
      this.ensure(transactionCtx, Check.IS_EXISTING, Check.IS_MANAGEABLE, Check.IS_NOT_COMPLETED);

      transactionCtx
          .update(TRAININGS)
          .set(TRAININGS.TRNG_OUTCOME, Constants.TRNG_OUTCOME_CANCELLED)
          .where(TRAININGS.TRNG_PK.eq(this.sessionId))
          .execute();
      transactionCtx
          .update(TRAININGS_EMPLOYEES)
          .set(TRAININGS_EMPLOYEES.TREM_OUTCOME, Constants.EMPL_OUTCOME_CANCELLED)
          .where(TRAININGS_EMPLOYEES.TREM_TRNG_FK.eq(this.sessionId))
          .execute();
    });
  }

  @POST
  @Path("{session}/reopen")
  public void reopenSession() {
    Transactions.with(this.ctx, transactionCtx -> {
      this.ensure(transactionCtx, Check.IS_EXISTING, Check.IS_MANAGEABLE, Check.IS_NOT_COMPLETED);

      transactionCtx
          .update(TRAININGS)
          .set(TRAININGS.TRNG_OUTCOME, Constants.TRNG_OUTCOME_SCHEDULED)
          .where(TRAININGS.TRNG_PK.eq(this.sessionId))
          .execute();
      transactionCtx
          .update(TRAININGS_EMPLOYEES)
          .set(TRAININGS_EMPLOYEES.TREM_OUTCOME, Constants.EMPL_OUTCOME_PENDING)
          .where(TRAININGS_EMPLOYEES.TREM_TRNG_FK.eq(this.sessionId))
          .execute();
    });
  }

  /**
   * Overrides any previous trainees registration on purpose.
   *
   * {@code trainees} should be like:
   *
   * <pre>
   * [{
   *    empl_pk: {@code <number>},
   *    trem_outcome: {@code <string>},
   *    trem_comment: {@code <string>}
   * }, {
   *    ...
   * }]
   * </pre>
   */
  @POST
  @Path("{session}/complete")
  @Consumes(MediaType.APPLICATION_JSON)
  public void completeSession(final List<Map<String, Object>> trainees) {
    Transactions.with(this.ctx, transactionCtx -> {
      final TrainingsRecord session = this.ensure(transactionCtx, Check.IS_EXISTING, Check.IS_MANAGEABLE, Check.IS_NOT_CANCELLED);
      final TrainingtypesDefsRecord definition = SessionsEndpoint
          .effectiveDefinition(transactionCtx, session.getTrngTrtyFk(), session.getTrngDate());
      final List<Integer> employeeIds = trainees.stream()
          .map(t -> (Integer) t.get(TRAININGS_EMPLOYEES.TREM_EMPL_FK.getName()))
          .collect(Collectors.toList());
      if (employeeIds.stream().distinct().count() != employeeIds.size()) {
        throw new IllegalArgumentException("A trainee can only occur once in a session completion request.");
      }
      SessionsEndpoint.validateOutcomes(
                                        definition,
                                        Constants.TRNG_OUTCOME_COMPLETED,
                                        trainees.stream()
                                            .map(t -> (String) t.get(TRAININGS_EMPLOYEES.TREM_OUTCOME.getName()))
                                            .collect(Collectors.toList()));

      transactionCtx
          .update(TRAININGS)
          .set(TRAININGS.TRNG_OUTCOME, Constants.TRNG_OUTCOME_COMPLETED)
          .where(TRAININGS.TRNG_PK.eq(this.sessionId))
          .execute();
      transactionCtx.deleteFrom(TRAININGS_EMPLOYEES).where(TRAININGS_EMPLOYEES.TREM_TRNG_FK.eq(this.sessionId)).execute();
      for (final Map<String, Object> trainee : trainees) {
        transactionCtx.insertInto(
                                  TRAININGS_EMPLOYEES,
                                  TRAININGS_EMPLOYEES.TREM_TRNG_FK,
                                  TRAININGS_EMPLOYEES.TREM_OUTCOME,
                                  TRAININGS_EMPLOYEES.TREM_COMMENT,
                                  TRAININGS_EMPLOYEES.TREM_EMPL_FK)
            .values(
                    this.sessionId.getValue(),
                    (String) trainee.get(TRAININGS_EMPLOYEES.TREM_OUTCOME.getName()),
                    (String) trainee.get(TRAININGS_EMPLOYEES.TREM_COMMENT.getName()),
                    (Integer) trainee.get(TRAININGS_EMPLOYEES.TREM_EMPL_FK.getName()))
            .execute();
      }
    });
  }

  @POST
  @Path("{session}/register")
  @Consumes(MediaType.APPLICATION_JSON)
  @SuppressWarnings("unchecked")
  public void registerTrainees(final List<Integer> trainees) {
    Transactions.with(this.ctx, transactionCtx -> {
      this.ensure(transactionCtx, Check.IS_EXISTING, Check.IS_MANAGEABLE, Check.IS_SCHEDULED);

      if (trainees.size() > 0) {
        final Table<Record1<Integer>> source = DSL.values(trainees.stream().map(t -> DSL.row(t)).toArray(Row1[]::new)).asTable();
        transactionCtx.insertInto(
                                  TRAININGS_EMPLOYEES,
                                  TRAININGS_EMPLOYEES.TREM_TRNG_FK,
                                  TRAININGS_EMPLOYEES.TREM_EMPL_FK,
                                  TRAININGS_EMPLOYEES.TREM_OUTCOME)
            .select(DSL.select(this.sessionId, source.field(0, Integer.class), DSL.val(Constants.EMPL_OUTCOME_PENDING)).from(source))
            .onDuplicateKeyIgnore()
            .execute();
      }
    });
  }

  @POST
  @Path("{session}/deregister")
  @Consumes(MediaType.APPLICATION_JSON)
  public void deregisterTrainees(final List<Integer> trainees) {
    Transactions.with(this.ctx, transactionCtx -> {
      this.ensure(transactionCtx, Check.IS_EXISTING, Check.IS_MANAGEABLE, Check.IS_SCHEDULED);

      transactionCtx.deleteFrom(TRAININGS_EMPLOYEES).where(TRAININGS_EMPLOYEES.TREM_TRNG_FK.eq(this.sessionId)
          .and(TRAININGS_EMPLOYEES.TREM_EMPL_FK.in(trainees)))
          .execute();
    });
  }

  @PUT
  @Path("{session}/trainees")
  @Consumes(MediaType.APPLICATION_JSON)
  @SuppressWarnings("unchecked")
  public void updateTrainees(final List<Integer> trainees) {
    Transactions.with(this.ctx, transactionCtx -> {
      this.ensure(transactionCtx, Check.IS_EXISTING, Check.IS_MANAGEABLE, Check.IS_SCHEDULED);

      if (trainees.size() > 0) {
        final Table<Record1<Integer>> source = DSL.values(trainees.stream().map(t -> DSL.row(t)).toArray(Row1[]::new)).asTable();
        transactionCtx.deleteFrom(TRAININGS_EMPLOYEES).where(TRAININGS_EMPLOYEES.TREM_TRNG_FK.eq(this.sessionId)
            .and(TRAININGS_EMPLOYEES.TREM_EMPL_FK.notIn(trainees)))
            .execute();
        transactionCtx.insertInto(
                                  TRAININGS_EMPLOYEES,
                                  TRAININGS_EMPLOYEES.TREM_TRNG_FK,
                                  TRAININGS_EMPLOYEES.TREM_EMPL_FK,
                                  TRAININGS_EMPLOYEES.TREM_OUTCOME)
            .select(DSL.select(this.sessionId, source.field(0, Integer.class), DSL.val(Constants.EMPL_OUTCOME_PENDING)).from(source))
            .onDuplicateKeyIgnore()
            .execute();
      }
    });
  }

  @POST
  @Path("bulk")
  @Deprecated
  @SuppressWarnings("unchecked")
  // TODO: Rewrite and move to BulkImportsEndpoint
  public void bulkImport(final List<Map<String, Object>> sessions) {
    Transactions.with(this.ctx, transactionCtx -> {
      for (final Map<String, Object> session : sessions) {
        final Integer type = (Integer) session.get(TRAININGS.TRNG_TRTY_FK.getName());
        final LocalDate date = LocalDate.parse(session.get(TRAININGS.TRNG_DATE.getName()).toString());
        final String outcome = (String) session.get(TRAININGS.TRNG_OUTCOME.getName());
        if (!this.restrictions.getManageableTypes().contains(type)) {
          throw new ForbiddenException();
        }

        final TrainingtypesDefsRecord definition = SessionsEndpoint.effectiveDefinition(transactionCtx, type, date);
        final Map<String, Map<String, String>> trainees = (Map<String, Map<String, String>>) session.getOrDefault("trainees", Collections.emptyMap());
        SessionsEndpoint.validateOutcomes(
                                          definition,
                                          outcome,
                                          trainees.values().stream()
                                              .map(trainee -> trainee.get(TRAININGS_EMPLOYEES.TREM_OUTCOME.getName()))
                                              .collect(Collectors.toList()));

        final Integer id = transactionCtx
            .insertInto(
                        TRAININGS,
                        TRAININGS.TRNG_TRTY_FK,
                        TRAININGS.TRNG_START,
                        TRAININGS.TRNG_DATE,
                        TRAININGS.TRNG_OUTCOME,
                        TRAININGS.TRNG_COMMENT)
            .values(
                    type,
                    session.get(TRAININGS.TRNG_START.getName()) != null ? LocalDate.parse(session.get(TRAININGS.TRNG_START.getName()).toString())
                                                                        : null,
                    date,
                    outcome,
                    (String) session.get(TRAININGS.TRNG_COMMENT.getName()))
            .returningResult(TRAININGS.TRNG_PK)
            .fetchOne().value1();

        trainees.forEach((trem_empl_fk, data) -> transactionCtx
                .insertInto(
                            TRAININGS_EMPLOYEES,
                            TRAININGS_EMPLOYEES.TREM_TRNG_FK,
                            TRAININGS_EMPLOYEES.TREM_EMPL_FK,
                            TRAININGS_EMPLOYEES.TREM_OUTCOME,
                            TRAININGS_EMPLOYEES.TREM_COMMENT)
                .values(
                        id,
                        Integer.valueOf(trem_empl_fk),
                        data.get(TRAININGS_EMPLOYEES.TREM_OUTCOME.getName()),
                        data.get(TRAININGS_EMPLOYEES.TREM_COMMENT.getName()))
                .execute());

        ((List<Integer>) session.getOrDefault("trainers", Collections.EMPTY_LIST))
            .forEach(instructor -> transactionCtx
                .insertInto(TRAININGS_TRAINERS, TRAININGS_TRAINERS.TRTR_TRNG_FK, TRAININGS_TRAINERS.TRTR_EMPL_FK)
                .values(id, instructor).execute());
      }
    });
  }

  private static void validateOutcomes(
                                       final TrainingtypesDefsRecord definition,
                                       final String sessionOutcome,
                                       final Iterable<String> traineeOutcomes) {
    if (!Constants.TRAINING_OUTCOMES.contains(sessionOutcome)) {
      throw new IllegalArgumentException(String.format("The outcome of a session must be one of: %s", Constants.TRAINING_OUTCOMES));
    }

    final List<String> allowed;
    switch (sessionOutcome) {
      case Constants.TRNG_OUTCOME_CANCELLED:
        allowed = Collections.singletonList(Constants.EMPL_OUTCOME_CANCELLED);
        break;
      case Constants.TRNG_OUTCOME_COMPLETED:
        allowed = Boolean.TRUE.equals(definition.getTtdfPresenceonly())
                                                                       ? SessionsEndpoint.PRESENCE_ONLY_COMPLETED_OUTCOMES
                                                                       : SessionsEndpoint.COMPLETED_OUTCOMES;
        break;
      default: // TRNG_OUTCOME_SCHEDULED
        allowed = Collections.singletonList(Constants.EMPL_OUTCOME_PENDING);
    }

    for (final String outcome : traineeOutcomes) {
      if (!allowed.contains(outcome)) {
        throw new IllegalArgumentException(String.format("Outcome for all trainees should be one of: %s", allowed));
      }
    }
  }

  private static TrainingtypesDefsRecord effectiveDefinition(final DSLContext transactionCtx, final Integer type, final LocalDate date) {
    return transactionCtx.selectFrom(TRAININGTYPES_DEFS)
        .where(TRAININGTYPES_DEFS.TTDF_PK.eq(Fields.selectTypeDefinition(DSL.val(type), DSL.val(date))))
        .fetchOptional()
        .orElseThrow(() -> new IllegalArgumentException(String
            .format("No session-type definition is effective for type %s on %s.", type, date)));
  }

  /**
   * Performs all the requested {@link Check}s, throwing an {@code Exception} as
   * soon as any of these fails.
   *
   * Actually, always ensures the session does exist and can be managed by the
   * user account authoring the request.
   */
  private TrainingsRecord ensure(final DSLContext transactionCtx, final Check... checks) {
    final Optional<TrainingsRecord> session = transactionCtx.selectFrom(TRAININGS).where(TRAININGS.TRNG_PK.eq(SessionsEndpoint.this.sessionId)).fetchOptional();
    if (!session.isPresent()) {
      throw new IllegalArgumentException("That session doesn't exist.");
    }

    if (!this.restrictions.getManageableTypes().contains(session.get().get(TRAININGS.TRNG_TRTY_FK))) {
      throw new ForbiddenException("This user account is not allowed to manage sessions of that type.");
    }

    final List<Check> checklist = Arrays.asList(checks);
    if (checklist.contains(Check.IS_SCHEDULED) && !Constants.TRNG_OUTCOME_SCHEDULED.equals(session.get().get(TRAININGS.TRNG_OUTCOME))) {
      throw new IllegalArgumentException("This session is not currently scheduled.");
    }

    if (checklist.contains(Check.IS_NOT_CANCELLED) && Constants.TRNG_OUTCOME_CANCELLED.equals(session.get().get(TRAININGS.TRNG_OUTCOME))) {
      throw new IllegalArgumentException("This session has been cancelled.");
    }

    if (checklist.contains(Check.IS_NOT_COMPLETED) && Constants.TRNG_OUTCOME_COMPLETED.equals(session.get().get(TRAININGS.TRNG_OUTCOME))) {
      throw new IllegalArgumentException("This session is already completed.");
    }
    return session.get();
  }
}
