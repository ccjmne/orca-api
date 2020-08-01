package org.ccjmne.orca.api.inject.core;

import static org.ccjmne.orca.jooq.codegen.Tables.CERTIFICATES;
import static org.ccjmne.orca.jooq.codegen.Tables.EMPLOYEES_VOIDINGS;
import static org.ccjmne.orca.jooq.codegen.Tables.SITES_EMPLOYEES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGS;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGTYPES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGTYPES_CERTIFICATES;

import java.math.BigDecimal;
import java.time.LocalDate;

import javax.inject.Inject;

import org.ccjmne.orca.api.inject.business.QueryParams;
import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.api.utils.Fields;
import org.eclipse.jdt.annotation.NonNull;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Select;
import org.jooq.SelectQuery;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.types.YearToMonth;

public class StatisticsSelection {

  private static final Integer DURATION_INFINITE = Integer.valueOf(0);

  public static final Field<LocalDate> EXPIRY = DSL.max(DSL
      .when(TRAININGTYPES_CERTIFICATES.TTCE_DURATION.eq(StatisticsSelection.DURATION_INFINITE), DSL.localDate(Constants.DATE_NEVER))
      .otherwise(TRAININGS.TRNG_DATE.plus(TRAININGTYPES_CERTIFICATES.TTCE_DURATION.mul(new YearToMonth(0, 1)))));

  // TODO: The number of months under which an aptitude is to be renewed soon
  // should be configurable per aptitude
  private static Field<String> fieldValidity(final Field<LocalDate> date) {
    return DSL
        .when(EMPLOYEES_VOIDINGS.EMVO_DATE.le(date), Constants.EMPL_STATUS_VOIDED)
        .when(StatisticsSelection.EXPIRY.ge(date.plus(new YearToMonth(0, 6))), Constants.EMPL_STATUS_LASTING)
        .when(StatisticsSelection.EXPIRY.ge(date), Constants.EMPL_STATUS_EXPIRING)
        .otherwise(Constants.EMPL_STATUS_EXPIRED);
  }

  private final Field<LocalDate> date;

  @Inject
  public StatisticsSelection(final QueryParams parameters) {
    this.date = parameters.get(QueryParams.DATE);
  }

  /**
   * For historicised statistics.
   *
   * @param date
   *          A mere reference to the {@code generate_series} dates
   */
  public StatisticsSelection(final Field<LocalDate> date) {
    this.date = date;
  }

  public Select<? extends Record> selectEmployeesStats() {
    return DSL
        .select(
                SITES_EMPLOYEES.SIEM_SITE_FK,
                TRAININGS_EMPLOYEES.TREM_EMPL_FK,
                TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK,
                Fields.formatDate(StatisticsSelection.EXPIRY).as("expiry"),
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
            .and(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(Fields.selectUpdate(this.date))))
        .where(TRAININGS_EMPLOYEES.TREM_OUTCOME.eq(Constants.EMPL_OUTCOME_VALIDATED))
        .and(TRAININGS.TRNG_DATE.le(this.date))
        .groupBy(
                 SITES_EMPLOYEES.SIEM_SITE_FK,
                 TRAININGS_EMPLOYEES.TREM_EMPL_FK,
                 TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK,
                 EMPLOYEES_VOIDINGS.EMVO_DATE);
  }

  @SuppressWarnings("null")
  public Select<? extends Record> selectSitesStats() {
    final Table<? extends Record> eStats = this.selectEmployeesStats().asTable();
    final Field<String> eStatus = eStats.field("status", String.class);
    final Table<? extends Record> sStats = DSL
        .select(
                eStats.field(SITES_EMPLOYEES.SIEM_SITE_FK),
                eStats.field(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK),
                DSL.count().filterWhere(eStatus.eq(Constants.EMPL_STATUS_LASTING)).as(Constants.EMPL_STATUS_LASTING),
                DSL.count().filterWhere(eStatus.eq(Constants.EMPL_STATUS_EXPIRING)).as(Constants.EMPL_STATUS_EXPIRING),
                DSL.count().filterWhere(eStatus.eq(Constants.EMPL_STATUS_EXPIRED)).as(Constants.EMPL_STATUS_EXPIRED),
                DSL.count().filterWhere(eStatus.eq(Constants.EMPL_STATUS_VOIDED)).as(Constants.EMPL_STATUS_VOIDED))
        .from(eStats)
        .groupBy(eStats.field(SITES_EMPLOYEES.SIEM_SITE_FK), eStats.field(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK))
        .asTable();

    final Field<Integer> eCount = DSL.count().as("of");
    final Table<? extends Record> certs = DSL
        .select(
                CERTIFICATES.CERT_PK,
                CERTIFICATES.CERT_TARGET,
                SITES_EMPLOYEES.SIEM_SITE_FK,
                eCount)
        .from(CERTIFICATES)
        .join(SITES_EMPLOYEES).on(DSL.noCondition())
        .and(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(Fields.selectUpdate(this.date)))
        .groupBy(CERTIFICATES.CERT_PK, SITES_EMPLOYEES.SIEM_SITE_FK)
        .asTable();

    final Field<Integer> okTarget = DSL.ceil(eCount.mul(certs.field(CERTIFICATES.CERT_TARGET).div(DSL.val(100f))));
    final Field<Integer> okayishTarget = DSL.ceil(eCount.mul(certs.field(CERTIFICATES.CERT_TARGET).mul(DSL.val(2f / 3)).div(DSL.val(100f))));
    final Field<Integer> validCount = DSL
        .coalesce(sStats.field(Constants.EMPL_STATUS_LASTING, Integer.class).plus(sStats.field(Constants.EMPL_STATUS_EXPIRING)), DSL.zero());
    final Field<Integer> invalidCount = DSL
        .coalesce(sStats.field(Constants.EMPL_STATUS_EXPIRED, Integer.class).plus(sStats.field(Constants.EMPL_STATUS_VOIDED)), DSL.zero());
    return DSL.select(
                      certs.field(eCount),
                      certs.field(SITES_EMPLOYEES.SIEM_SITE_FK),
                      certs.field(CERTIFICATES.CERT_PK),
                      DSL.coalesce(sStats.field(Constants.EMPL_STATUS_LASTING), DSL.zero()).as(Constants.EMPL_STATUS_LASTING),
                      DSL.coalesce(sStats.field(Constants.EMPL_STATUS_EXPIRING), DSL.zero()).as(Constants.EMPL_STATUS_EXPIRING),
                      DSL.coalesce(sStats.field(Constants.EMPL_STATUS_EXPIRED), DSL.zero()).as(Constants.EMPL_STATUS_EXPIRED),
                      DSL.coalesce(sStats.field(Constants.EMPL_STATUS_VOIDED), DSL.zero()).as(Constants.EMPL_STATUS_VOIDED),
                      validCount.as(Constants.EMPL_STATUS_VALID),
                      invalidCount.as(Constants.EMPL_STATUS_INVALID),
                      okTarget.as("target"),
                      DSL.when(okTarget.minus(validCount).ge(DSL.zero()), okTarget.minus(validCount)).otherwise(DSL.zero()).as("missing"),
                      DSL.round(validCount.mul(Float.valueOf(100f)).div(eCount), 1).as("percent"),
                      DSL
                          .when(validCount.ge(okTarget), Constants.SITE_STATUS_OK)
                          .when(validCount.ge(okayishTarget), Constants.SITE_STATUS_OKAYISH)
                          .otherwise(Constants.SITE_STATUS_KO)
                          .as("status"))
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
  public SelectQuery<? extends Record> selectSitesGroupsStats() {
    final Table<? extends Record> sitesStats = this.selectSitesStats().asTable();

    final Field<@NonNull String> status = sitesStats.field("status", String.class);
    final Field<BigDecimal> score = DSL.round(DSL
        .sum(DSL
            .when(status.eq(Constants.SITE_STATUS_OK), DSL.val(1f))
            .when(status.eq(Constants.SITE_STATUS_OKAYISH), DSL.val(2f / 3))
            .otherwise(DSL.val(0f)))
        .div(DSL.count())
        .mul(DSL.val(100))
        .cast(SQLDataType.NUMERIC), 1);

    try (final SelectQuery<Record> q = DSL.select().getQuery()) {
      q.addSelect(sitesStats.field(CERTIFICATES.CERT_PK));
      q.addSelect(
                  DSL.round(DSL.sum(sitesStats.field(Constants.EMPL_STATUS_VALID, Integer.class))
                      .div(DSL.sum(sitesStats.field("of", Integer.class)))
                      .mul(Integer.valueOf(100)), 1).as("percent"),
                  score.as("score"),
                  DSL.sum(sitesStats.field(Constants.EMPL_STATUS_LASTING, Integer.class)).as(Constants.EMPL_STATUS_LASTING),
                  DSL.sum(sitesStats.field(Constants.EMPL_STATUS_EXPIRING, Integer.class)).as(Constants.EMPL_STATUS_EXPIRING),
                  DSL.sum(sitesStats.field(Constants.EMPL_STATUS_EXPIRED, Integer.class)).as(Constants.EMPL_STATUS_EXPIRED),
                  DSL.sum(sitesStats.field(Constants.EMPL_STATUS_VOIDED, Integer.class)).as(Constants.EMPL_STATUS_VOIDED),
                  DSL.sum(sitesStats.field(Constants.EMPL_STATUS_VALID, Integer.class)).as(Constants.EMPL_STATUS_VALID),
                  DSL.sum(sitesStats.field(Constants.EMPL_STATUS_INVALID, Integer.class)).as(Constants.EMPL_STATUS_INVALID),
                  DSL.count().filterWhere(status.eq(Constants.SITE_STATUS_OK)).as(Constants.SITE_STATUS_OK),
                  DSL.count().filterWhere(status.eq(Constants.SITE_STATUS_OKAYISH)).as(Constants.SITE_STATUS_OKAYISH),
                  DSL.count().filterWhere(status.eq(Constants.SITE_STATUS_KO)).as(Constants.SITE_STATUS_KO),
                  DSL.when(score.eq(DSL.val(BigDecimal.valueOf(100))), Constants.SITE_STATUS_OK)
                      .when(score.ge(DSL.val(BigDecimal.valueOf(67))), Constants.SITE_STATUS_OKAYISH)
                      .otherwise(Constants.SITE_STATUS_KO)
                      .as("status"));
      q.addFrom(sitesStats);
      q.addGroupBy(sitesStats.field(CERTIFICATES.CERT_PK));
      return q;
    }
  }

  public Select<? extends Record> selectSessionsStats() {
    return DSL.select(TRAININGS_EMPLOYEES.TREM_TRNG_FK, TRAININGS_EMPLOYEES.TREM_OUTCOME, DSL.count().as("count"))
        .from(TRAININGS_EMPLOYEES).groupBy(TRAININGS_EMPLOYEES.TREM_TRNG_FK, TRAININGS_EMPLOYEES.TREM_OUTCOME);
  }
}
