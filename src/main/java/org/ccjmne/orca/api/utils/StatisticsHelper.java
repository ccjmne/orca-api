package org.ccjmne.orca.api.utils;

import static org.ccjmne.orca.jooq.classes.Tables.CERTIFICATES;
import static org.ccjmne.orca.jooq.classes.Tables.EMPLOYEES_VOIDINGS;
import static org.ccjmne.orca.jooq.classes.Tables.SITES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES_TAGS;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGS_TRAINERS;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGTYPES;
import static org.ccjmne.orca.jooq.classes.Tables.TRAININGTYPES_CERTIFICATES;

import java.math.BigDecimal;
import java.sql.Date;

import org.eclipse.jdt.annotation.NonNull;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.JoinType;
import org.jooq.Record;
import org.jooq.Select;
import org.jooq.SelectQuery;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.types.DayToSecond;
import org.jooq.types.YearToMonth;

public class StatisticsHelper {

	private static final Integer DURATION_INFINITE = Integer.valueOf(0);

	private static final Field<Date> MAX_EXPIRY = DSL.max(DSL
			.when(TRAININGTYPES_CERTIFICATES.TTCE_DURATION.eq(StatisticsHelper.DURATION_INFINITE), DSL.date(Constants.DATE_INFINITY))
			.otherwise(TRAININGS.TRNG_DATE.plus(TRAININGTYPES_CERTIFICATES.TTCE_DURATION.mul(new YearToMonth(0, 1)))));

	private static final Field<Date> EXPIRY = DSL
			.when(
					EMPLOYEES_VOIDINGS.EMVO_DATE.le(StatisticsHelper.MAX_EXPIRY),
					EMPLOYEES_VOIDINGS.EMVO_DATE.sub(new DayToSecond(1)))
			.otherwise(StatisticsHelper.MAX_EXPIRY);

	private static Field<String> fieldValidity(final String dateStr) {
		return DSL
				.when(
						StatisticsHelper.EXPIRY.ge(Constants.fieldDate(dateStr).plus(new YearToMonth(0, 6))),
						Constants.STATUS_SUCCESS)
				.when(
						StatisticsHelper.EXPIRY.ge(Constants.fieldDate(dateStr)),
						Constants.STATUS_WARNING)
				.otherwise(Constants.STATUS_DANGER);
	}

	private static Field<Date> fieldOptedOut(final String dateStr) {
		return DSL.field(EMPLOYEES_VOIDINGS.EMVO_DATE);
	}

	/**
	 * The <code>Condition</code> should be on
	 * <code>TRAININGS_EMPLOYEES.TREM_EMPL_FK</code>.
	 */
	public static Select<? extends Record> selectEmployeesStats(
																final String dateStr,
																final Condition employeesSelection) {
		return DSL
				.select(
						SITES_EMPLOYEES.SIEM_SITE_FK,
						TRAININGS_EMPLOYEES.TREM_EMPL_FK,
						TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK,
						StatisticsHelper.EXPIRY.as("expiry"),
						StatisticsHelper.fieldOptedOut(dateStr).as("opted_out"),
						StatisticsHelper.fieldValidity(dateStr).as("validity"))
				.from(TRAININGTYPES_CERTIFICATES)
				.join(TRAININGTYPES).on(TRAININGTYPES.TRTY_PK.eq(TRAININGTYPES_CERTIFICATES.TTCE_TRTY_FK))
				.join(TRAININGS).on(TRAININGS.TRNG_TRTY_FK.eq(TRAININGTYPES.TRTY_PK))
				.join(TRAININGS_EMPLOYEES).on(TRAININGS_EMPLOYEES.TREM_TRNG_FK.eq(TRAININGS.TRNG_PK))
				.leftJoin(EMPLOYEES_VOIDINGS)
				.on(EMPLOYEES_VOIDINGS.EMVO_EMPL_FK.eq(TRAININGS_EMPLOYEES.TREM_EMPL_FK)
						.and(EMPLOYEES_VOIDINGS.EMVO_CERT_FK.eq(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK)))
				.join(SITES_EMPLOYEES)
				.on(SITES_EMPLOYEES.SIEM_EMPL_FK.eq(TRAININGS_EMPLOYEES.TREM_EMPL_FK)
						.and(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(Constants.selectUpdate(dateStr))))
				.where(TRAININGS_EMPLOYEES.TREM_OUTCOME.eq(Constants.EMPL_OUTCOME_VALIDATED))
				.and(TRAININGS.TRNG_DATE.le(Constants.fieldDate(dateStr)))
				.and(employeesSelection)
				.groupBy(
							SITES_EMPLOYEES.SIEM_SITE_FK,
							TRAININGS_EMPLOYEES.TREM_EMPL_FK,
							TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK,
							EMPLOYEES_VOIDINGS.EMVO_DATE);
	}

	/**
	 * The <code>employeesSelection</code> condition should be on
	 * <code>TRAININGS_EMPLOYEES.TREM_EMPL_FK</code>.<br />
	 * The <code>sitesSelection</code> condition should be on
	 * <code>SITES_EMPLOYEES.SIEM_SITE_FK</code>.
	 */
	@SuppressWarnings("null")
	public static Select<? extends Record> selectSitesStats(
															final String dateStr,
															final Condition employeesSelection,
															final Condition sitesSelection) {
		final Table<? extends Record> employeesStats = StatisticsHelper
				.selectEmployeesStats(dateStr, employeesSelection)
				.asTable();

		final Table<? extends Record> certificatesStats = DSL
				.select(
						employeesStats.field(SITES_EMPLOYEES.SIEM_SITE_FK),
						employeesStats.field(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK),
						DSL.count(employeesStats.field(TRAININGS_EMPLOYEES.TREM_EMPL_FK))
								.filterWhere(employeesStats.field("validity", String.class).eq(Constants.STATUS_SUCCESS))
								.as(Constants.STATUS_SUCCESS),
						DSL.count(employeesStats.field(TRAININGS_EMPLOYEES.TREM_EMPL_FK))
								.filterWhere(employeesStats.field("validity", String.class).eq(Constants.STATUS_WARNING))
								.as(Constants.STATUS_WARNING),
						DSL.count(employeesStats.field(TRAININGS_EMPLOYEES.TREM_EMPL_FK))
								.filterWhere(employeesStats.field("validity", String.class).eq(Constants.STATUS_DANGER))
								.as(Constants.STATUS_DANGER))
				.from(employeesStats)
				.groupBy(employeesStats.field(SITES_EMPLOYEES.SIEM_SITE_FK), employeesStats.field(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK))
				.asTable();

		final Table<? extends Record> certificates = DSL
				.select(
						SITES_EMPLOYEES.SIEM_SITE_FK,
						CERTIFICATES.CERT_PK,
						CERTIFICATES.CERT_TARGET,
						DSL.count().as("employeesCount"))
				.from(SITES_EMPLOYEES)
				.rightJoin(CERTIFICATES).on(DSL.val(true))
				.where(sitesSelection)
				.and(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(Constants.selectUpdate(dateStr)))
				.groupBy(SITES_EMPLOYEES.SIEM_SITE_FK, CERTIFICATES.CERT_PK, CERTIFICATES.CERT_TARGET)
				.asTable();

		final Field<Integer> targetCount = DSL.ceil(certificates.field("employeesCount", Integer.class)
				.mul(certificates.field(CERTIFICATES.CERT_TARGET).div(DSL.val(100f))));
		final Field<Integer> warningTargetCount = DSL.ceil(certificates.field("employeesCount", Integer.class)
				.mul(certificates.field(CERTIFICATES.CERT_TARGET).div(DSL.val(300 / 2f))));
		final Field<Integer> validCount = DSL
				.coalesce(
							certificatesStats.field(Constants.STATUS_SUCCESS, Integer.class).add(certificatesStats.field(Constants.STATUS_WARNING)),
							Integer.valueOf(0));
		return DSL.select(
							certificates.field(SITES_EMPLOYEES.SIEM_SITE_FK),
							certificates.field(CERTIFICATES.CERT_PK),
							DSL.coalesce(certificatesStats.field(Constants.STATUS_SUCCESS, Integer.class), Integer.valueOf(0)).as(Constants.STATUS_SUCCESS),
							DSL.coalesce(certificatesStats.field(Constants.STATUS_WARNING, Integer.class), Integer.valueOf(0)).as(Constants.STATUS_WARNING),
							DSL.coalesce(certificatesStats.field(Constants.STATUS_DANGER, Integer.class), Integer.valueOf(0)).as(Constants.STATUS_DANGER),
							targetCount.as("target"),
							DSL
									.when(validCount.ge(targetCount), Constants.STATUS_SUCCESS)
									.when(validCount.ge(warningTargetCount), Constants.STATUS_WARNING)
									.otherwise(Constants.STATUS_DANGER).as("validity"))
				.from(certificatesStats)
				.rightJoin(certificates)
				.on(certificates.field(CERTIFICATES.CERT_PK).eq(certificatesStats.field(TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK))
						.and(certificates.field(SITES_EMPLOYEES.SIEM_SITE_FK).eq(certificatesStats.field(SITES_EMPLOYEES.SIEM_SITE_FK))))
				.join(SITES).on(SITES.SITE_PK.eq(certificates.field(SITES_EMPLOYEES.SIEM_SITE_FK)));
	}

	/**
	 * The <code>employeesSelection</code> condition should be on
	 * <code>TRAININGS_EMPLOYEES.TREM_EMPL_FK</code>.<br />
	 * The <code>sitesSelection</code> condition should be on
	 * <code>SITES_EMPLOYEES.SIEM_SITE_FK</code>.<br />
	 * The <code>tagsSelection</code> condition should be on <code>
	 * SITES_TAGS.SITA_TAGS_FK</code>
	 */
	@SuppressWarnings("null")
	public static Select<? extends Record> selectSitesGroupsStats(
																	final String dateStr,
																	final Condition employeesSelection,
																	final Condition sitesSelection,
																	final Integer tags_pk) {
		final Table<? extends Record> sitesStats = StatisticsHelper
				.selectSitesStats(dateStr, employeesSelection, sitesSelection)
				.asTable();

		final Field<@NonNull String> validityStatus = sitesStats.field("validity", String.class);
		final Field<BigDecimal> score = DSL.round(DSL
				.sum(DSL
						.when(validityStatus.eq(Constants.STATUS_SUCCESS), DSL.val(1f))
						.when(validityStatus.eq(Constants.STATUS_WARNING), DSL.val(2 / 3f))
						.otherwise(DSL.val(0f)))
				.mul(DSL.val(100)).div(DSL.count()));

		try (final SelectQuery<Record> q = DSL.select().getQuery()) {
			q.addSelect(sitesStats.field(CERTIFICATES.CERT_PK));
			q.addSelect(
						DSL.sum(sitesStats.field(Constants.STATUS_SUCCESS, Integer.class)).as(Constants.STATUS_SUCCESS),
						DSL.sum(sitesStats.field(Constants.STATUS_WARNING, Integer.class)).as(Constants.STATUS_WARNING),
						DSL.sum(sitesStats.field(Constants.STATUS_DANGER, Integer.class)).as(Constants.STATUS_DANGER),
						DSL.count().filterWhere(validityStatus.eq(Constants.STATUS_SUCCESS))
								.as("sites_" + Constants.STATUS_SUCCESS),
						DSL.count().filterWhere(validityStatus.eq(Constants.STATUS_WARNING))
								.as("sites_" + Constants.STATUS_WARNING),
						DSL.count().filterWhere(validityStatus.eq(Constants.STATUS_DANGER))
								.as("sites_" + Constants.STATUS_DANGER),
						score.as("score"),
						DSL.when(score.eq(DSL.val(BigDecimal.valueOf(100))), Constants.STATUS_SUCCESS)
								.when(score.ge(DSL.val(BigDecimal.valueOf(67))), Constants.STATUS_WARNING)
								.otherwise(Constants.STATUS_DANGER).as("validity"));

			q.addFrom(sitesStats);

			if (tags_pk != null) {
				// Non-tagged sites appear under TAGS_VALUE_NONE
				q.addSelect(ResourcesHelper.coalesce(SITES_TAGS.SITA_VALUE, Constants.TAGS_VALUE_NONE));
				q.addJoin(
							SITES_TAGS,
							JoinType.LEFT_OUTER_JOIN,
							SITES_TAGS.SITA_SITE_FK.eq(sitesStats.field(SITES_EMPLOYEES.SIEM_SITE_FK))
									.and(SITES_TAGS.SITA_TAGS_FK.eq(tags_pk)));
			} else {
				// All sites marked as TAGS_VALUE_UNIVERSAL
				q.addSelect(DSL.val(Constants.TAGS_VALUE_UNIVERSAL).as(SITES_TAGS.SITA_VALUE));
				q.addJoin(SITES_TAGS, JoinType.LEFT_OUTER_JOIN, DSL.condition(Boolean.FALSE));
			}

			q.addGroupBy(SITES_TAGS.SITA_VALUE, sitesStats.field(CERTIFICATES.CERT_PK));
			return q;
		}
	}

	public static final Field<Integer> TRAINING_REGISTERED = DSL.count(TRAININGS_EMPLOYEES.TREM_PK).as("registered");
	public static final Field<Integer> TRAINING_VALIDATED = DSL.count(TRAININGS_EMPLOYEES.TREM_OUTCOME)
			.filterWhere(TRAININGS_EMPLOYEES.TREM_OUTCOME.eq(Constants.EMPL_OUTCOME_VALIDATED)).as("validated");
	public static final Field<Integer> TRAINING_FLUNKED = DSL.count(TRAININGS_EMPLOYEES.TREM_OUTCOME)
			.filterWhere(TRAININGS_EMPLOYEES.TREM_OUTCOME.eq(Constants.EMPL_OUTCOME_FLUNKED)).as("flunked");
	public static final Field<Integer> TRAINING_MISSING = DSL.count(TRAININGS_EMPLOYEES.TREM_OUTCOME)
			.filterWhere(TRAININGS_EMPLOYEES.TREM_OUTCOME.eq(Constants.EMPL_OUTCOME_MISSING)).as("missing");
	public static final Field<String> TRAINING_TRAINERS = DSL.select(DSL.arrayAgg(TRAININGS_TRAINERS.TRTR_EMPL_FK)).from(TRAININGS_TRAINERS)
			.where(TRAININGS_TRAINERS.TRTR_TRNG_FK.eq(TRAININGS.TRNG_PK)).asField("trainers");
}
