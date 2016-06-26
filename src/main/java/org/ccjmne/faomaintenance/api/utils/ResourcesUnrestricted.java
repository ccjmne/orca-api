package org.ccjmne.faomaintenance.api.utils;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.CERTIFICATES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGTYPES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGTYPES_CERTIFICATES;

import java.sql.Date;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.ccjmne.faomaintenance.api.rest.ResourcesEndpoint;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.CertificatesRecord;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.TrainingtypesCertificatesRecord;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.TrainingtypesRecord;
import org.jooq.DSLContext;
import org.jooq.JoinType;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SelectQuery;
import org.jooq.impl.DSL;

public class ResourcesUnrestricted {
	private final DSLContext ctx;

	@Inject
	public ResourcesUnrestricted(final DSLContext ctx) {
		this.ctx = ctx;
	}

	public Result<TrainingtypesRecord> listTrainingTypes() {
		return this.ctx.selectFrom(TRAININGTYPES).orderBy(TRAININGTYPES.TRTY_ORDER).fetch();
	}

	public Result<TrainingtypesCertificatesRecord> listTrainingTypesCertificates() {
		return this.ctx.selectFrom(TRAININGTYPES_CERTIFICATES).fetch();
	}

	public Result<CertificatesRecord> listCertificates() {
		return this.ctx.selectFrom(CERTIFICATES).orderBy(CERTIFICATES.CERT_ORDER).fetch();
	}

	/**
	 * Used solely by the {@link StatisticsCaches} to build statistics for
	 * employees or sites.<br />
	 * Lists <b>past</b> trainings of all types. This implementation <b>does not
	 * filter out results</b> based off of the current
	 * {@link HttpServletRequest}'s restrictions.<br />
	 */
	public Result<Record> listTrainings(final String empl_pk) {
		final Date now = new Date(new java.util.Date().getTime());
		try (final SelectQuery<Record> query = this.ctx.selectQuery()) {
			query.addSelect(TRAININGS.fields());
			query.addFrom(TRAININGS);
			query.addGroupBy(TRAININGS.fields());
			query.addJoin(TRAININGS_EMPLOYEES, JoinType.LEFT_OUTER_JOIN, TRAININGS_EMPLOYEES.TREM_TRNG_FK.eq(TRAININGS.TRNG_PK));

			query.addSelect(Constants.TRAINING_REGISTERED);
			query.addSelect(Constants.TRAINING_VALIDATED);
			query.addSelect(Constants.TRAINING_FLUNKED);
			query.addSelect(Constants.TRAINING_EXPIRY);
			query.addSelect(Constants.TRAINING_TRAINERS);

			if (empl_pk != null) {
				query.addSelect(TRAININGS_EMPLOYEES.TREM_OUTCOME, TRAININGS_EMPLOYEES.TREM_COMMENT);
				query.addGroupBy(TRAININGS_EMPLOYEES.TREM_OUTCOME, TRAININGS_EMPLOYEES.TREM_COMMENT);
				query.addConditions(TRAININGS_EMPLOYEES.TREM_PK
						.in(DSL.select(TRAININGS_EMPLOYEES.TREM_PK).from(TRAININGS_EMPLOYEES).where(TRAININGS_EMPLOYEES.TREM_EMPL_FK.eq(empl_pk))));
			}

			query.addConditions(TRAININGS.TRNG_DATE.le(now).or(TRAININGS.TRNG_START.isNotNull().and(TRAININGS.TRNG_START.le(now))));

			query.addOrderBy(TRAININGS.TRNG_DATE);
			return query.fetch();
		}
	}

	/**
	 * Used solely by the {@link StatisticsCaches} to build statistics for
	 * sites.<br />
	 * Lists employees according to a lot of parameters just like
	 * {@link ResourcesEndpoint#listEmployees(String, String, String)} , except
	 * that this implementation <b>does not filter out results</b> based off of
	 * the current {@link HttpServletRequest}'s restrictions.<br />
	 */
	public Result<Record> listEmployees(final String site_pk) {
		try (final SelectQuery<Record> query = this.ctx.selectQuery()) {
			query.addSelect(
							EMPLOYEES.EMPL_PK,
							EMPLOYEES.EMPL_FIRSTNAME,
							EMPLOYEES.EMPL_SURNAME,
							EMPLOYEES.EMPL_DOB,
							EMPLOYEES.EMPL_PERMANENT,
							EMPLOYEES.EMPL_GENDER,
							EMPLOYEES.EMPL_NOTES,
							EMPLOYEES.EMPL_ADDR);
			query.addSelect(SITES_EMPLOYEES.fields());
			query.addFrom(EMPLOYEES);
			if (site_pk != null) {
				query.addJoin(
								SITES_EMPLOYEES,
								SITES_EMPLOYEES.SIEM_EMPL_FK.eq(EMPLOYEES.EMPL_PK),
								SITES_EMPLOYEES.SIEM_SITE_FK.eq(site_pk),
								SITES_EMPLOYEES.SIEM_UPDT_FK.eq(Constants.LATEST_UPDATE));
			} else {
				query.addJoin(
								SITES_EMPLOYEES,
								SITES_EMPLOYEES.SIEM_EMPL_FK.eq(EMPLOYEES.EMPL_PK),
								SITES_EMPLOYEES.SIEM_SITE_FK.ne(Constants.UNASSIGNED_SITE),
								SITES_EMPLOYEES.SIEM_UPDT_FK.eq(Constants.LATEST_UPDATE));
			}

			return query.fetch();
		}
	}
}
