package org.ccjmne.orca.api.demo;

import static org.ccjmne.orca.jooq.codegen.Tables.EMPLOYEES;
import static org.ccjmne.orca.jooq.codegen.Tables.SITES;
import static org.ccjmne.orca.jooq.codegen.Tables.SITES_EMPLOYEES;
import static org.ccjmne.orca.jooq.codegen.Tables.SITES_TAGS;
import static org.ccjmne.orca.jooq.codegen.Tables.TAGS;
import static org.ccjmne.orca.jooq.codegen.Tables.UPDATES;

import java.time.LocalDate;
import java.time.Month;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.jooq.codegen.tables.records.EmployeesRecord;
import org.ccjmne.orca.jooq.codegen.tables.records.SitesRecord;
import org.ccjmne.orca.jooq.codegen.tables.records.SitesTagsRecord;
import org.jooq.DSLContext;
import org.jooq.Insert;
import org.jooq.Record1;
import org.jooq.Row1;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.types.YearToMonth;

public class DemoDataSitesEmployees {

  public static void generate(final DSLContext ctx) {
    ctx.batchInsert(DemoDataSitesEmployees.addSites(200)).execute();
    ctx.batchInsert(DemoDataSitesEmployees.addEmployees(5000)).execute();

    // GEO tags
    ctx.execute(DemoDataSitesEmployees
        .insertTags(
                    ctx.insertInto(TAGS, TAGS.TAGS_NAME, TAGS.TAGS_SHORT, TAGS.TAGS_TYPE, TAGS.TAGS_HEX_COLOUR)
                        .values("Situation Géographique", "GEO", Constants.TAGS_TYPE_STRING, "#C71585").returning(TAGS.TAGS_PK).fetchOne()
                        .get(TAGS.TAGS_PK),
                    "ABCDEF".chars()
                        .mapToObj(c -> String.format("Zone %s", Character.valueOf((char) c)))
                        .map(DSL::row)
                        .toArray(Row1[]::new)));

    // TYPE tags
    ctx.execute(DemoDataSitesEmployees
        .insertTags(
                    ctx.insertInto(TAGS, TAGS.TAGS_NAME, TAGS.TAGS_SHORT, TAGS.TAGS_TYPE, TAGS.TAGS_HEX_COLOUR)
                        .values("Type d'Activité", "TYPE", Constants.TAGS_TYPE_STRING, "#795548").returning(TAGS.TAGS_PK).fetchOne()
                        .get(TAGS.TAGS_PK),
                    Arrays.asList("Usine", "Bureaux", "Entrepôt", "Boutique").stream().map(DSL::row).toArray(Row1[]::new)));

    // ERP tags
    ctx.execute(DemoDataSitesEmployees
        .insertTags(
                    ctx.insertInto(TAGS, TAGS.TAGS_NAME, TAGS.TAGS_SHORT, TAGS.TAGS_TYPE, TAGS.TAGS_HEX_COLOUR)
                        .values("Établissement Recevant du Public", "ERP", Constants.TAGS_TYPE_BOOLEAN, "#009688")
                        .returning(TAGS.TAGS_PK).fetchOne().get(TAGS.TAGS_PK),
                    Arrays.asList(String.valueOf(Boolean.TRUE), String.valueOf(Boolean.FALSE)).stream()
                        .map(DSL::row)
                        .toArray(Row1[]::new)));

    // Update dated from NCLS Development's DOB :)
    final Integer update = ctx.insertInto(UPDATES, UPDATES.UPDT_DATE).values(LocalDate.of(2014, Month.DECEMBER, 8)).returning(UPDATES.UPDT_PK)
        .fetchOne()
        .get(UPDATES.UPDT_PK);

    ctx.insertInto(SITES_EMPLOYEES, SITES_EMPLOYEES.SIEM_UPDT_FK, SITES_EMPLOYEES.SIEM_SITE_FK, SITES_EMPLOYEES.SIEM_EMPL_FK)
        .select(DSL.select(DSL.val(update), DSL.field("site_pk", Integer.class), DSL.field("empl_pk", Integer.class))
            .from(DSL.select(
                             EMPLOYEES.EMPL_PK,
                             DSL.floor(DSL.rand().mul(DSL.select(DSL.count()).from(SITES).where(SITES.SITE_PK.ne(Constants.DECOMMISSIONED_SITE))
                                 .asField()).plus(DSL.one()))
                                 .as("linked_site_id"))
                .from(EMPLOYEES).where(EMPLOYEES.EMPL_PK.ne(Constants.EMPLOYEE_ROOT)).asTable("employees_view"))
            .join(DSL.select(
                             SITES.SITE_PK,
                             DSL.rowNumber().over().orderBy(DSL.rand()).as("site_id"))
                .from(SITES).where(SITES.SITE_PK.ne(Constants.DECOMMISSIONED_SITE)).asTable("sites_view"))
            .on(DSL.field("linked_site_id").eq(DSL.field("site_id"))))
        .execute();

    // Another update dated from a year ago
    final Integer oneYearAgo = ctx.insertInto(UPDATES, UPDATES.UPDT_DATE).values(DSL.currentLocalDate().minus(new YearToMonth(1)))
        .returning(UPDATES.UPDT_PK)
        .fetchOne()
        .get(UPDATES.UPDT_PK);

    ctx.insertInto(SITES_EMPLOYEES, SITES_EMPLOYEES.SIEM_UPDT_FK, SITES_EMPLOYEES.SIEM_SITE_FK, SITES_EMPLOYEES.SIEM_EMPL_FK)
        .select(DSL.select(DSL.val(oneYearAgo), DSL.field("site_pk", Integer.class), DSL.field("empl_pk", Integer.class))
            .from(DSL.select(
                             EMPLOYEES.EMPL_PK,
                             DSL.floor(DSL.rand().mul(DSL.select(DSL.count()).from(SITES).where(SITES.SITE_PK.ne(Constants.DECOMMISSIONED_SITE))
                                 .asField()).plus(DSL.one()))
                                 .as("linked_site_id"))
                .from(EMPLOYEES).where(EMPLOYEES.EMPL_PK.ne(Constants.EMPLOYEE_ROOT)).asTable("employees_view"))
            .join(DSL.select(
                             SITES.SITE_PK,
                             DSL.rowNumber().over().orderBy(DSL.rand()).as("site_id"))
                .from(SITES).where(SITES.SITE_PK.ne(Constants.DECOMMISSIONED_SITE)).asTable("sites_view"))
            .on(DSL.field("linked_site_id").eq(DSL.field("site_id"))))
        .execute();

    // Another update dated from six months ago
    final Integer sixMonthsAgo = ctx.insertInto(UPDATES, UPDATES.UPDT_DATE).values(DSL.currentLocalDate().minus(new YearToMonth(0, 6)))
        .returning(UPDATES.UPDT_PK)
        .fetchOne()
        .get(UPDATES.UPDT_PK);

    ctx.insertInto(SITES_EMPLOYEES, SITES_EMPLOYEES.SIEM_UPDT_FK, SITES_EMPLOYEES.SIEM_SITE_FK, SITES_EMPLOYEES.SIEM_EMPL_FK)
        .select(DSL.select(DSL.val(sixMonthsAgo), DSL.field("site_pk", Integer.class), DSL.field("empl_pk", Integer.class))
            .from(DSL.select(
                             EMPLOYEES.EMPL_PK,
                             DSL.floor(DSL.rand().mul(DSL.select(DSL.count()).from(SITES).where(SITES.SITE_PK.ne(Constants.DECOMMISSIONED_SITE))
                                 .asField()).plus(DSL.one()))
                                 .as("linked_site_id"))
                .from(EMPLOYEES).where(EMPLOYEES.EMPL_PK.ne(Constants.EMPLOYEE_ROOT)).asTable("employees_view"))
            .join(DSL.select(
                             SITES.SITE_PK,
                             DSL.rowNumber().over().orderBy(DSL.rand()).as("site_id"))
                .from(SITES).where(SITES.SITE_PK.ne(Constants.DECOMMISSIONED_SITE)).asTable("sites_view"))
            .on(DSL.field("linked_site_id").eq(DSL.field("site_id"))))
        .execute();
  }

  public static Insert<SitesTagsRecord> insertTags(final Integer tags_pk, final Row1<String>[] values) {
    final Table<Record1<String>> tagsTable = DSL.values(values).asTable();
    return DSL.insertInto(SITES_TAGS, SITES_TAGS.SITA_SITE_FK, SITES_TAGS.SITA_TAGS_FK, SITES_TAGS.SITA_VALUE)
        .select(DSL.select(DSL.field("site", Integer.class), DSL.val(tags_pk), DSL.field("tag", String.class))
            .from(DSL
                .select(SITES.SITE_PK.as("site"),
                        DSL.rowNumber().over().orderBy(DSL.rand()).mod(Integer.valueOf(values.length)).plus(DSL.one()).as("tag_fk"))
                .from(SITES).where(SITES.SITE_PK.ne(Constants.DECOMMISSIONED_SITE)))
            .join(DSL.select(tagsTable.field(0, String.class).as("tag"), DSL.rowNumber().over().as("tag_pk")).from(tagsTable))
            .on(DSL.field("tag_fk").eq(DSL.field("tag_pk"))));
  }

  private static List<EmployeesRecord> addEmployees(final int amount) {
    return IntStream.rangeClosed(1, amount).mapToObj(new FakeRecords()::employee).peek(empl -> empl.reset(EMPLOYEES.EMPL_PK)).collect(Collectors.toList());
  }

  private static List<SitesRecord> addSites(final int amount) {
    return IntStream.rangeClosed(1, amount).mapToObj(new FakeRecords()::site).peek(site -> site.reset(SITES.SITE_PK)).collect(Collectors.toList());
  }
}
