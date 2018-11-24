package org.ccjmne.orca.api.utils;

import static org.ccjmne.orca.jooq.classes.Tables.CERTIFICATES;
import static org.ccjmne.orca.jooq.classes.Tables.EMPLOYEES_VOIDINGS;
import static org.ccjmne.orca.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS_TRAINERS;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGTYPES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGTYPES_CERTIFICATES;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.Optional;

import javax.inject.Inject;

import org.ccjmne.orca.api.inject.QueryParameters;
import org.eclipse.jdt.annotation.NonNull;
import org.jooq.Field;
import org.jooq.Param;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Row1;
import org.jooq.SelectFinalStep;
import org.jooq.SelectQuery;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.types.DayToSecond;
import org.jooq.types.YearToMonth;

// TODO: Move to orca.api.inject
public class StatisticsSelection {

  @SuppressWarnings("unchecked")
  private static final Table<Record1<String>> OUTCOMES_TABLE = DSL
      .values(Constants.EMPLOYEES_OUTCOMES.stream().map(DSL::row).toArray(Row1[]::new))
      .asTable("employees_outcomes", "outcome");

  private static final Integer DURATION_INFINITE = Integer.valueOf(0);

  public static final Field<Date> MAX_EXPIRY = DSL.max(DSL
      .when(TRAININGTYPES_CERTIFICATES.TTCE_DURATION.eq(StatisticsSelection.DURATION_INFINITE), DSL.date(Constants.DATE_INFINITY))
      .otherwise(TRAININGS.TRNG_DATE.plus(TRAININGTYPES_CERTIFICATES.TTCE_DURATION.mul(new YearToMonth(0, 1)))));

  private static final Field<Date> EXPIRY = DSL
      .when(EMPLOYEES_VOIDINGS.EMVO_DATE.le(StatisticsSelection.MAX_EXPIRY), EMPLOYEES_VOIDINGS.EMVO_DATE.sub(new DayToSecond(1)))
      .otherwise(StatisticsSelection.MAX_EXPIRY);

  private static Field<String> fieldValidity(final Optional<Param<Date>> date) {
    return DSL
        .when(StatisticsSelection.EXPIRY.ge(Constants.fieldDate(date).plus(new YearToMonth(0, 6))), Constants.STATUS_SUCCESS)
        .when(StatisticsSelection.EXPIRY.ge(Constants.fieldDate(date)), Constants.STATUS_WARNING)
        .otherwise(Constants.STATUS_DANGER);
  }

  private final Optional<Param<Date>> date;

  @Inject
  public StatisticsSelection(final QueryParameters parameters) {
    this.date = parameters.of(QueryParameters.DATE);
  }

  public SelectFinalStep<? extends Record> selectEmployeesStats() {
    return DSL
        .select(
                SITES_EMPLOYEES.SIEM_SITE_FK,
                TRAININGS_EMPLOYEES.TREM_EMPL_FK,
                TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK,
                ResourcesHelper.formatDate(StatisticsSelection.EXPIRY).as("expiry"),
                DSL.field(EMPLOYEES_VOIDINGS.EMVO_DATE).as("void_since"),
                StatisticsSelection.fieldValidity(this.date).as("status"))
        .from(TRAININGTYPES_CERTIFICATES)
        .join(TRAININGTYPES).on(TRAININGTYPES.TRTY_PK.eq(TRAININGTYPES_CERTIFICATES.TTCE_TRTY_FK))
        .join(TRAININGS).on(TRAININGS.TRNG_TRTY_FK.eq(TRAININGTYPES.TRTY_PK))
        .join(TRAININGS_EMPLOYEES).on(TRAININGS_EMPLOYEES.TREM_TRNG_FK.eq(TRAININGS.TRNG_PK))
        .leftJoin(EMPLOYEES_VOIDINGS)
        .on(EMPLOYEES_VOIDINGS.EMVO_EMPL_FK.eq(TRAININGS_EMPLOYEES.TREM_EMPL_FK)
            .and(EMPLOYEES_VOIDINGS.EMVO_CERT_FK.eq(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK)))
        .join(SITES_EMPLOYEES)
        .on(SITES_EMPLOYEES.SIEM_EMPL_FK.eq(TRAININGS_EMPLOYEES.TREM_EMPL_FK)
            .and(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(Constants.selectUpdate(this.date))))
        .where(TRAININGS_EMPLOYEES.TREM_OUTCOME.eq(Constants.EMPL_OUTCOME_VALIDATED))
        .and(TRAININGS.TRNG_DATE.le(Constants.fieldDate(this.date)))
        .groupBy(
                 SITES_EMPLOYEES.SIEM_SITE_FK,
                 TRAININGS_EMPLOYEES.TREM_EMPL_FK,
                 TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK,
                 EMPLOYEES_VOIDINGS.EMVO_DATE);
  }

  @SuppressWarnings("null")
  public SelectFinalStep<? extends Record> selectSitesStats() {
    final Table<? extends Record> eStats = this.selectEmployeesStats().asTable();
    final Field<String> eStatus = eStats.field("status", String.class);
    final Table<? extends Record> sStats = DSL
        .select(
                eStats.field(SITES_EMPLOYEES.SIEM_SITE_FK),
                eStats.field(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK),
                DSL.count().filterWhere(eStatus.eq(Constants.STATUS_SUCCESS)).as(Constants.STATUS_SUCCESS),
                DSL.count().filterWhere(eStatus.eq(Constants.STATUS_WARNING)).as(Constants.STATUS_WARNING),
                DSL.count().filterWhere(eStatus.eq(Constants.STATUS_DANGER)).as(Constants.STATUS_DANGER))
        .from(eStats)
        .groupBy(eStats.field(SITES_EMPLOYEES.SIEM_SITE_FK), eStats.field(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK))
        .asTable();

    final Field<Integer> eCount = DSL.count().as("site_employees");
    final Table<? extends Record> certs = DSL
        .select(
                CERTIFICATES.CERT_PK,
                CERTIFICATES.CERT_TARGET,
                SITES_EMPLOYEES.SIEM_SITE_FK,
                eCount)
        .from(CERTIFICATES)
        .join(SITES_EMPLOYEES).on(DSL.trueCondition())
        // TODO: Use DSL.noCondition() when upgrading jOOQ
        .and(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(Constants.selectUpdate(this.date)))
        .groupBy(CERTIFICATES.CERT_PK, SITES_EMPLOYEES.SIEM_SITE_FK)
        .asTable();

    final Field<Integer> target = DSL.ceil(eCount.mul(certs.field(CERTIFICATES.CERT_TARGET).div(DSL.val(100f))));
    final Field<Integer> warningTarget = DSL.ceil(eCount.mul(certs.field(CERTIFICATES.CERT_TARGET).div(DSL.val(300 / 2f))));
    final Field<Integer> current = DSL
        .coalesce(sStats.field(Constants.STATUS_SUCCESS, Integer.class).add(sStats.field(Constants.STATUS_WARNING)),
                  Integer.valueOf(0));
    return DSL.select(
                      certs.field(eCount),
                      certs.field(SITES_EMPLOYEES.SIEM_SITE_FK),
                      certs.field(CERTIFICATES.CERT_PK),
                      DSL.coalesce(sStats.field(Constants.STATUS_SUCCESS, Integer.class), Integer.valueOf(0)).as(Constants.STATUS_SUCCESS),
                      DSL.coalesce(sStats.field(Constants.STATUS_WARNING, Integer.class), Integer.valueOf(0)).as(Constants.STATUS_WARNING),
                      DSL.coalesce(sStats.field(Constants.STATUS_DANGER, Integer.class), Integer.valueOf(0)).as(Constants.STATUS_DANGER),
                      current.as("current"),
                      target.as("target"),
                      DSL
                          .when(current.ge(target), Constants.STATUS_SUCCESS)
                          .when(current.ge(warningTarget), Constants.STATUS_WARNING)
                          .otherwise(Constants.STATUS_DANGER).as("status"))
        .from(certs)
        .leftOuterJoin(sStats)
        .on(sStats.field(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK).eq(certs.field(CERTIFICATES.CERT_PK))
            .and(sStats.field(SITES_EMPLOYEES.SIEM_SITE_FK).eq(certs.field(SITES_EMPLOYEES.SIEM_SITE_FK))));
  }

  /**
   * <strong>Warning:</strong> This one's a special one. It can't be
   * {@code JOIN}'d to a
   * "sites-groups"-yielding query, since doing so would grant no way of only
   * selecting certain sites within those groups.<br />
   * <br />
   * For that reason, the query built with this method, ran by itself, would build
   * the statistics for all the selected data, as one universal blob.<br />
   * <br />
   * It needs to be {@code JOIN}'d to the relevant sites first (for filtering),
   * then actually {@code GROUP BY}'d the sites-group identifier.
   */
  @SuppressWarnings("null")
  public SelectQuery<? extends Record> selectSitesGroupsStats() {
    final Table<? extends Record> sitesStats = this.selectSitesStats().asTable();

    final Field<@NonNull String> status = sitesStats.field("status", String.class);
    final Field<BigDecimal> score = DSL.round(DSL
        .sum(DSL
            .when(status.eq(Constants.STATUS_SUCCESS), DSL.val(1f))
            .when(status.eq(Constants.STATUS_WARNING), DSL.val(2 / 3f))
            .otherwise(DSL.val(0f)))
        .mul(DSL.val(100)).div(DSL.count()));

    try (final SelectQuery<Record> q = DSL.select().getQuery()) {
      q.addSelect(sitesStats.field(CERTIFICATES.CERT_PK));
      q.addSelect(
                  DSL.sum(sitesStats.field("current", Integer.class)).as("current"),
                  score.as("score"),
                  DSL.sum(sitesStats.field(Constants.STATUS_SUCCESS, Integer.class)).as(Constants.STATUS_SUCCESS),
                  DSL.sum(sitesStats.field(Constants.STATUS_WARNING, Integer.class)).as(Constants.STATUS_WARNING),
                  DSL.sum(sitesStats.field(Constants.STATUS_DANGER, Integer.class)).as(Constants.STATUS_DANGER),
                  DSL.count().filterWhere(status.eq(Constants.STATUS_SUCCESS))
                      .as("sites_" + Constants.STATUS_SUCCESS),
                  DSL.count().filterWhere(status.eq(Constants.STATUS_WARNING))
                      .as("sites_" + Constants.STATUS_WARNING),
                  DSL.count().filterWhere(status.eq(Constants.STATUS_DANGER))
                      .as("sites_" + Constants.STATUS_DANGER),
                  DSL.when(score.eq(DSL.val(BigDecimal.valueOf(100))), Constants.STATUS_SUCCESS)
                      .when(score.ge(DSL.val(BigDecimal.valueOf(67))), Constants.STATUS_WARNING)
                      .otherwise(Constants.STATUS_DANGER).as("status"));
      q.addFrom(sitesStats);
      q.addGroupBy(sitesStats.field(CERTIFICATES.CERT_PK));
      return q;
    }
  }

  public static SelectQuery<? extends Record> selectSessionsStats() {
    final Field<@NonNull String> outcome = OUTCOMES_TABLE.field("outcome", String.class);
    return DSL
        .select(TRAININGS_EMPLOYEES.TREM_TRNG_FK, outcome, DSL.count().as("count"))
        .from(OUTCOMES_TABLE)
        .leftOuterJoin(TRAININGS_EMPLOYEES).on(TRAININGS_EMPLOYEES.TREM_OUTCOME.eq(outcome))
        .groupBy(TRAININGS_EMPLOYEES.TREM_TRNG_FK, outcome).getQuery();
  }

  // TODO: Delete all these when rewriting sessions statistics module
  public static final Field<Integer> TRAINING_REGISTERED = DSL.count(TRAININGS_EMPLOYEES.TREM_PK).as("registered");
  public static final Field<Integer> TRAINING_VALIDATED  = DSL.count(TRAININGS_EMPLOYEES.TREM_OUTCOME)
      .filterWhere(TRAININGS_EMPLOYEES.TREM_OUTCOME.eq(Constants.EMPL_OUTCOME_VALIDATED)).as("validated");
  public static final Field<Integer> TRAINING_FLUNKED    = DSL.count(TRAININGS_EMPLOYEES.TREM_OUTCOME)
      .filterWhere(TRAININGS_EMPLOYEES.TREM_OUTCOME.eq(Constants.EMPL_OUTCOME_FLUNKED)).as("flunked");
  public static final Field<Integer> TRAINING_MISSING    = DSL.count(TRAININGS_EMPLOYEES.TREM_OUTCOME)
      .filterWhere(TRAININGS_EMPLOYEES.TREM_OUTCOME.eq(Constants.EMPL_OUTCOME_MISSING)).as("missing");
  public static final Field<String>  TRAINING_TRAINERS   = DSL.select(DSL.arrayAgg(TRAININGS_TRAINERS.TRTR_EMPL_FK)).from(TRAININGS_TRAINERS)
      .where(TRAININGS_TRAINERS.TRTR_TRNG_FK.eq(TRAININGS.TRNG_PK)).asField("trainers");
}
