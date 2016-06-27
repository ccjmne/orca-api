package org.ccjmne.faomaintenance.api.utils;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.CERTIFICATES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES_CERTIFICATES_OPTOUT;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGTYPES_CERTIFICATES;

import java.sql.Date;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.ccjmne.faomaintenance.api.rest.resources.EmployeeStatistics;
import org.ccjmne.faomaintenance.api.rest.resources.EmployeeStatistics.EmployeeStatisticsBuilder;
import org.ccjmne.faomaintenance.api.rest.resources.SiteStatistics;
import org.jooq.DSLContext;
import org.jooq.Record;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class StatisticsCaches {
	private final DSLContext ctx;
	private final ResourcesUnrestricted unrestrictedResources;

	private final LoadingCache<String, Map.Entry<Date, EmployeeStatistics>> employeeStatisticsCache;
	private final LoadingCache<String, Map.Entry<Date, SiteStatistics>> siteStatisticsCache;

	@Inject
	public StatisticsCaches(
							final DSLContext ctx,
							final ResourcesUnrestricted unrestrictedResources) {
		this.ctx = ctx;
		this.unrestrictedResources = unrestrictedResources;

		final ExecutorService computingExecutor = Executors.newFixedThreadPool(16);

		this.employeeStatisticsCache = CacheBuilder.newBuilder()
				.refreshAfterWrite(30, TimeUnit.MINUTES)
				.<String, Entry<Date, EmployeeStatistics>> build(CacheLoader.asyncReloading(CacheLoader.<String, Entry<Date, EmployeeStatistics>> from(key -> {
					return StatisticsCaches.this.buildLatestEmployeeStats(key);
				}), computingExecutor));

		this.siteStatisticsCache = CacheBuilder.newBuilder()
				.refreshAfterWrite(1, TimeUnit.HOURS)
				.<String, Entry<Date, SiteStatistics>> build(CacheLoader.asyncReloading(CacheLoader.<String, Map.Entry<Date, SiteStatistics>> from(key -> {
					return StatisticsCaches.this.calculateLatestSiteStats(key);
				}), computingExecutor));
		// TODO: pre-fill caches on init
	}

	public Entry<Date, SiteStatistics> getSiteStats(final String site_pk) {
		return this.siteStatisticsCache.getUnchecked(site_pk);
	}

	public Entry<Date, EmployeeStatistics> getEmployeeStats(final String empl_pk) {
		return this.employeeStatisticsCache.getUnchecked(empl_pk);
	}

	public void invalidateSitesStats() {
		this.siteStatisticsCache.invalidateAll();
	}

	public void invalidateSitesStats(final Collection<String> sites) {
		this.siteStatisticsCache.invalidateAll(sites);
	}

	public void invalidateEmployeesStats() {
		this.employeeStatisticsCache.invalidateAll();
		this.siteStatisticsCache.invalidateAll();
	}

	/**
	 * Allowed to directly use the {@link DSLContext} with no access
	 * restriction.<br />
	 * Reason: <b>Cache management</b>.
	 */
	public void invalidateEmployeesStats(final Collection<String> employees) {
		this.employeeStatisticsCache.invalidateAll(employees);
		invalidateSitesStats(this.ctx
				.selectDistinct(SITES_EMPLOYEES.SIEM_SITE_FK)
				.from(SITES_EMPLOYEES)
				.where(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(Constants.LATEST_UPDATE)
						.and(SITES_EMPLOYEES.SIEM_EMPL_FK.in(employees)))
				.fetch(SITES_EMPLOYEES.SIEM_SITE_FK));
	}

	public void putSitesStats(final String site_pk, final Date date, final SiteStatistics stats) {
		this.siteStatisticsCache.put(site_pk, new SimpleEntry<>(date, stats));
	}

	private Map.Entry<Date, SiteStatistics> calculateLatestSiteStats(final String site_pk) {
		final Date currentDate = new Date(new java.util.Date().getTime());
		final SiteStatistics stats = new SiteStatistics(this.unrestrictedResources.listCertificates().intoMap(CERTIFICATES.CERT_PK));
		for (final Record empl : this.unrestrictedResources.listEmployees(site_pk)) {
			stats.register(
							empl.getValue(EMPLOYEES.EMPL_PK),
							empl.getValue(EMPLOYEES.EMPL_PERMANENT),
							this.employeeStatisticsCache.getUnchecked(empl.getValue(EMPLOYEES.EMPL_PK)).getValue());
		}

		return new SimpleEntry<>(currentDate, stats);
	}

	private Map.Entry<Date, EmployeeStatistics> buildLatestEmployeeStats(final String empl_pk) {
		final Date now = new Date(new java.util.Date().getTime());
		final EmployeeStatisticsBuilder builder = EmployeeStatistics
				.builder(
							this.unrestrictedResources.listTrainingTypesCertificates()
									.intoGroups(TRAININGTYPES_CERTIFICATES.TTCE_TRTY_FK, TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK),
							this.unrestrictedResources.listCertificatesVoiding(empl_pk)
									.intoMap(EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_CERT_FK, EMPLOYEES_CERTIFICATES_OPTOUT.EMCE_DATE));

		this.unrestrictedResources.listTrainings(empl_pk).forEach(training -> builder.accept(training));
		return new SimpleEntry<>(now, builder.buildFor(now));
	}
}
