package org.ccjmne.orca.api.rest.admin;

import static org.ccjmne.orca.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.SITES_TAGS;
import static org.ccjmne.orca.jooq.classes.Tables.UPDATES;
import static org.ccjmne.orca.jooq.classes.Tables.USERS;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

import org.ccjmne.orca.api.modules.Restrictions;
import org.ccjmne.orca.api.rest.fetch.ResourcesEndpoint;
import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.api.utils.ResourcesHelper;
import org.ccjmne.orca.api.utils.Transactions;
import org.ccjmne.orca.jooq.classes.tables.records.EmployeesRecord;
import org.ccjmne.orca.jooq.classes.tables.records.SitesRecord;
import org.eclipse.jdt.annotation.NonNull;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.Record3;
import org.jooq.Row2;
import org.jooq.Row3;
import org.jooq.Select;
import org.jooq.Table;
import org.jooq.TableRecord;
import org.jooq.impl.DSL;

import com.google.common.collect.ImmutableList;

@Path("bulk-import")
public class BulkImportsEndpoint {

	private final DSLContext ctx;

	@Inject
	public BulkImportsEndpoint(final DSLContext ctx, final ResourcesEndpoint resources, final Restrictions restrictions) {
		if (!restrictions.canManageSitesAndTags()) {
			throw new ForbiddenException();
		}

		this.ctx = ctx;
	}

	@POST
	@Path("employees")
	@SuppressWarnings("unchecked")
	public void bulkImportEmployees(final List<Map<String, String>> employees) {
		Transactions.with(this.ctx, transactionCtx -> {
			// 1. 'UPSERT' employees
			final Map<Boolean, List<@NonNull EmployeesRecord>> records = employees.stream().map(BulkImportsEndpoint.as(EmployeesRecord.class))
					.peek(BulkImportsEndpoint.setIfExists(	EMPLOYEES.EMPL_PK, EMPLOYEES.EMPL_EXTERNAL_ID,
															transactionCtx.select(EMPLOYEES.EMPL_EXTERNAL_ID, EMPLOYEES.EMPL_PK).from(EMPLOYEES)
																	.where(EMPLOYEES.EMPL_PK.ne(Constants.EMPLOYEE_ROOT))
																	.fetchMap(EMPLOYEES.EMPL_EXTERNAL_ID, EMPLOYEES.EMPL_PK)))
					.collect(Collectors.partitioningBy(r -> r.changed(EMPLOYEES.EMPL_PK)));
			transactionCtx.batchUpdate(records.get(Boolean.TRUE)).execute();
			transactionCtx.batchInsert(records.get(Boolean.FALSE)).execute();

			// 2. INSERT newer update -- no more than ONE per day
			transactionCtx.delete(UPDATES).where(UPDATES.UPDT_DATE.eq(DSL.currentDate())).execute();
			final Integer update = transactionCtx.insertInto(UPDATES).set(UPDATES.UPDT_DATE, DSL.currentDate()).returning(UPDATES.UPDT_PK).fetchOne()
					.getValue(UPDATES.UPDT_PK);

			// 3. INSERT sites-employees for newer update
			final Table<Record2<String, String>> allocations = DSL
					.values(employees.stream().<Row2<String, String>> map(e -> DSL
							.<String, String> row(e.get(EMPLOYEES.EMPL_EXTERNAL_ID.getName()), e.get(SITES.SITE_EXTERNAL_ID.getName())))
							.toArray(Row2[]::new))
					.asTable("unused", "employee", "site");
			transactionCtx.insertInto(SITES_EMPLOYEES, SITES_EMPLOYEES.SIEM_EMPL_FK, SITES_EMPLOYEES.SIEM_SITE_FK, SITES_EMPLOYEES.SIEM_UPDT_FK)
					.select(DSL.select(EMPLOYEES.EMPL_PK, SITES.SITE_PK, DSL.value(update))
							.from(allocations
									.join(EMPLOYEES).on(EMPLOYEES.EMPL_EXTERNAL_ID.eq(allocations.field("employee", String.class)))
									.join(SITES).on(SITES.SITE_EXTERNAL_ID.eq(allocations.field("site", String.class)))))
					.execute();

			// 4. Explicitly set missing employees' site to DECOMMISSIONED_SITE
			try (final Select<Record1<Integer>> active = DSL.selectDistinct(EMPLOYEES.EMPL_PK).from(EMPLOYEES)
					.join(SITES_EMPLOYEES).on(SITES_EMPLOYEES.SIEM_EMPL_FK.eq(EMPLOYEES.EMPL_PK))
					.where(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(update)).and(SITES_EMPLOYEES.SIEM_SITE_FK.ne(Constants.DECOMMISSIONED_SITE))) {
				transactionCtx.insertInto(SITES_EMPLOYEES, SITES_EMPLOYEES.SIEM_EMPL_FK, SITES_EMPLOYEES.SIEM_SITE_FK, SITES_EMPLOYEES.SIEM_UPDT_FK)
						.select(DSL.select(EMPLOYEES.EMPL_PK, DSL.val(Constants.DECOMMISSIONED_SITE), DSL.val(update))
								.from(EMPLOYEES)
								.where(EMPLOYEES.EMPL_PK.notIn(active)))
						.execute();

				// 5. DELETE corresponding users
				transactionCtx
						.deleteFrom(USERS)
						.where(USERS.USER_TYPE.eq(Constants.USERTYPE_EMPLOYEE))
						.and(USERS.USER_EMPL_FK.notIn(active))
						.and(USERS.USER_ID.ne(Constants.USER_ROOT))
						.execute();

				transactionCtx
						.deleteFrom(USERS)
						.where(USERS.USER_TYPE.eq(Constants.USERTYPE_SITE))
						.and(USERS.USER_SITE_FK.notIn(DSL
								.selectDistinct(SITES_EMPLOYEES.SIEM_SITE_FK).from(SITES_EMPLOYEES).where(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(update))))
						.execute();
			}
		});
	}

	@POST
	@Path("sites")
	@SuppressWarnings("unchecked")
	public void bulkImportSites(final List<Map<String, String>> sites) {
		Transactions.with(this.ctx, transactionCtx -> {
			// 1. 'UPSERT' sites
			final Map<Boolean, List<SitesRecord>> records = sites.stream().map(BulkImportsEndpoint.as(SitesRecord.class))
					.peek(BulkImportsEndpoint.setIfExists(	SITES.SITE_PK, SITES.SITE_EXTERNAL_ID,
															transactionCtx.select(SITES.SITE_EXTERNAL_ID, SITES.SITE_PK).from(SITES)
																	.where(SITES.SITE_PK.ne(Constants.DECOMMISSIONED_SITE))
																	.fetchMap(SITES.SITE_EXTERNAL_ID, SITES.SITE_PK)))
					.collect(Collectors.partitioningBy(r -> r.changed(SITES.SITE_PK)));
			transactionCtx.batchUpdate(records.get(Boolean.TRUE)).execute();
			transactionCtx.batchInsert(records.get(Boolean.FALSE)).execute();

			// 2. DELETE obsolete sites
			try (final Select<Record1<Integer>> delete = DSL.select(SITES.SITE_PK).from(SITES)
					.where(SITES.SITE_PK.ne(Constants.DECOMMISSIONED_SITE))
					.and(SITES.SITE_EXTERNAL_ID.notIn(sites.stream().map(s -> s.get(SITES.SITE_EXTERNAL_ID.getName())).collect(Collectors.toSet())))) {
				// matching SITES_EMPLOYEES records link to DECOMMISSIONED_SITE
				transactionCtx.update(SITES_EMPLOYEES).set(SITES_EMPLOYEES.SIEM_SITE_FK, Constants.DECOMMISSIONED_SITE)
						.where(SITES_EMPLOYEES.SIEM_SITE_FK.in(delete)).execute();
				transactionCtx.deleteFrom(SITES).where(SITES.SITE_PK.in(delete)).execute();
			}

			// 3. TRUNCATE and INSERT tags
			transactionCtx.truncate(SITES_TAGS).restartIdentity().execute();
			final Table<Record3<String, Integer, String>> tags = DSL.<String, Integer, String> values(sites.stream().flatMap(s -> s.entrySet().stream()
					.filter(e -> ResourcesHelper.IS_TAG_KEY.test(e.getKey()))
					.peek(e -> {
						if (Constants.TAGS_VALUE_NONE.equals(e.getValue()) || Constants.TAGS_VALUE_UNIVERSAL.equals(e.getValue())) {
							throw new IllegalArgumentException(String
									.format("Invalid tag value: '%s' for site: %s", e.getValue(), s.get(SITES.SITE_EXTERNAL_ID.getName())));
						}
					})
					.reduce(ImmutableList.<Row3<String, Integer, String>> builder(),
							(l, e) -> l.add(DSL.row(s.get(SITES.SITE_EXTERNAL_ID.getName()), Integer.valueOf(e.getKey()), e.getValue())),
							(l, l2) -> l.addAll(l2.build()))
					.build().stream()).toArray(Row3[]::new)).asTable("unused", "site", "tag", "value");
			transactionCtx
					.insertInto(SITES_TAGS, SITES_TAGS.SITA_SITE_FK, SITES_TAGS.SITA_TAGS_FK, SITES_TAGS.SITA_VALUE)
					.select(DSL.select(SITES.SITE_PK, tags.field("tag", Integer.class), tags.field("value", String.class))
							.from(tags).join(SITES).on(SITES.SITE_EXTERNAL_ID.eq(tags.field("site", String.class))))
					.execute();
		});
	}

	@SuppressWarnings("null")
	private static <R extends TableRecord<?>> Function<Map<String, String>, R> as(final Class<R> recordType) {
		return m -> {
			try {
				final R record = recordType.newInstance();
				record.fromMap(m);
				return record;
			} catch (InstantiationException | IllegalAccessException e) {
				// Can not happen with <R extends TableRecord>
				throw new RuntimeException(e);
			}
		};
	}

	@SuppressWarnings("null")
	private static final <R extends TableRecord<?>, T> Consumer<R> setIfExists(final Field<T> set, final Field<String> find, final Map<String, T> source) {
		return record -> {
			if (source.containsKey(record.get(find))) {
				record.set(set, source.get(record.get(find)));
			} else {
				record.reset(set);
			}
		};
	}
}
