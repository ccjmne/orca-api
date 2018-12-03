package org.ccjmne.orca.api.utils;

import static org.ccjmne.orca.jooq.classes.Tables.SITES_TAGS;
import static org.ccjmne.orca.jooq.classes.Tables.TAGS;
import static org.ccjmne.orca.jooq.classes.Tables.UPDATES;
import static org.ccjmne.orca.jooq.classes.Tables.USERS;

import java.sql.Date;
import java.util.Arrays;

import org.ccjmne.orca.jooq.classes.tables.records.UpdatesRecord;
import org.jooq.Field;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.Select;
import org.jooq.SelectQuery;
import org.jooq.Table;
import org.jooq.TableLike;
import org.jooq.Transaction;
import org.jooq.impl.DSL;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Streams;

/**
 * A collection of generic Fields helper methods that are of use in many places.
 *
 * @author ccjmne
 */
public class Fields {

  private static final Integer NO_UPDATE = Integer.valueOf(-1);

  public static Field<?>[] USERS_FIELDS              = new Field<?>[] { USERS.USER_ID, USERS.USER_TYPE, USERS.USER_EMPL_FK, USERS.USER_SITE_FK };
  public static String[]   EMPLOYEES_STATS_FIELDS    = new String[] { "status", "expiry", "void_since" };
  public static String[]   SITES_STATS_FIELDS        = new String[] { "status", "current", "target", "success", "warning", "danger" };
  public static String[]   SITES_GROUPS_STATS_FIELDS = new String[] {
      "status", "current", "score", "success", "warning", "danger", "sites_success", "sites_warning", "sites_danger" };

  /**
   * The {@link SITES_TAGS#SITA_VALUE} field coerced to either a boolean or a
   * string JSON element.
   */
  public static final Field<JsonNode> TAG_VALUE_COERCED = DSL
      .when(Fields.unqualify(TAGS.TAGS_TYPE).eq(Constants.TAGS_TYPE_BOOLEAN),
            JSONFields.toJson(DSL.cast(Fields.unqualify(SITES_TAGS.SITA_VALUE), Boolean.class)))
      .otherwise(JSONFields.toJson(Fields.unqualify(SITES_TAGS.SITA_VALUE)));

  /**
   * Returns a sub-query selecting the <strong>primary key</strong> of the
   * {@link UpdatesRecord} that is or was relevant at a given date, or today
   * if no date is specified.
   *
   * @param date
   *          The date for which to compute the relevant
   *          {@link UpdatesRecord}, in the <code>"YYYY-MM-DD"</code>
   *          format.
   * @return The relevant {@link UpdatesRecord}'s primary key or
   *         {@value Constants#NO_UPDATE} if no such update found.
   */
  public static Field<Integer> selectUpdate(final Field<Date> date) {
    return DSL.coalesce(DSL.select(UPDATES.UPDT_PK).from(UPDATES).where(UPDATES.UPDT_DATE.eq(DSL.select(DSL.max(UPDATES.UPDT_DATE)).from(UPDATES)
        .where(UPDATES.UPDT_DATE.le(date)))).asField(), NO_UPDATE);
  }

  /**
   * Formats a date-type {@link Field} as a {@link String}. Handles the case
   * where the date is "never" (for certificate expiration, for instance).
   *
   * @param field
   *          The {@code Field<java.sql.Date>} to be formatted
   * @return A new {@code VARCHAR}-type {@code Field}, formatted as our API
   *         formats {@link Date}s
   */
  public static Field<String> formatDate(final Field<java.sql.Date> field) {
    return DSL
        .when(field.eq(DSL.inline(Constants.DATE_INFINITY)), Constants.DATE_INFINITY_LITERAL)
        .otherwise(DSL.field("to_char({0}, {1})", String.class, field, APIDateFormat.FORMAT));
  }

  /**
   * Uses the {@code unaccent} PostgreSQL extension function to remove accents for
   * a given {@code field}.
   *
   * @param field
   *          The field to be unaccented
   * @return The unaccented {@code field}
   */
  public static Field<String> unaccent(final Field<String> field) {
    return DSL.function("unaccent", String.class, field);
  }

  /**
   * Returns a select sub-query that maps the results of the provided
   * {@code query} to the sole specified {@code field}.
   *
   * @param <T>
   *          The specified field type.
   * @param field
   *          The field to extract.
   * @param query
   *          The original query to use as a data source.
   * @return A sub-query containing the sole specified {@code field} from the
   *         given {@code query}.
   */
  public static <T> Select<Record1<T>> select(final Field<T> field, final SelectQuery<?> query) {
    final TableLike<? extends Record> table = query.asTable();
    return DSL.select(table.field(field)).from(table);
  }

  /**
   * Clone the supplied {@code fields} stripped from their {@code Table}
   * qualification, for referencing within sub-queries.<br />
   * Alternatively, use {@link TableLike#fields(Field...)}.
   *
   * @param fields
   *          The array of {@code Field}s to be cloned
   * @return The un-qualified {@code fields}
   */
  @SafeVarargs
  public static <T> Field<? extends T>[] unqualify(final Field<? extends T>... fields) {
    return Arrays.stream(fields).map(Field::getUnqualifiedName).map(DSL::field).toArray(Field[]::new);
  }

  /**
   * Clone the supplied {@code field} stripped from its {@code Table}
   * qualification, for referencing within sub-queries.<br />
   * Alternatively, use {@link TableLike#field(Field)}.
   *
   * @param field
   *          The {@code Field}s to be cloned
   * @return The un-qualified {@code field}
   */
  public static <T> Field<T> unqualify(final Field<T> field) {
    return DSL.field(field.getUnqualifiedName(), field.getType());
  }

  /**
   * Concatenates {@code fields} argument and {@code moreFields} into a single
   * array.
   *
   * @return A collection of all the concatenated {@link Field}s
   */
  @SafeVarargs
  public static <T> Field<? extends T>[] concat(final Field<? extends T>[] fields, final Field<? extends T>... moreFields) {
    return Streams.concat(Arrays.stream(fields), Arrays.stream(moreFields)).toArray(Field[]::new);
  }

  /**
   * Concatenates {@code fields} arguments into a single array.
   *
   * @return A collection of all the concatenated {@link Field}s
   */
  @SafeVarargs
  public static <T> Field<? extends T>[] concat(final Field<? extends T>[]... fields) {
    return Arrays.stream(fields).flatMap(Arrays::stream).toArray(Field[]::new);
  }

  /**
   * Generates a {@link Query} that will remove duplicates and reintroduce
   * missing entries in the virtual sequence of ordering values stored as a
   * {@link Table}'s {@link Field}.
   *
   * @param table
   *          The {@link Table} whose ordering field to cleanup
   * @param key
   *          A {@link Field} acting as unique ID for the records in this
   *          table (most likely its <em>primary key</em>)
   * @param order
   *          The {@link Field} by which this table's records are to be
   *          ordered
   * @return A {@link Query} to be executed, ideally within a
   *         {@link Transaction}
   */
  public static Query cleanupSequence(final Table<?> table, final Field<Integer> key, final Field<Integer> order) {
    final Field<Integer> newOrder = DSL.rowNumber().over().orderBy(order).as("new_order");
    final Table<Record2<Integer, Integer>> reorderMap = DSL
        .select(key, newOrder)
        .from(table).asTable("reorder_map");

    return DSL.update(table).set(order, reorderMap.field(newOrder))
        .from(reorderMap)
        .where(key.eq(reorderMap.field(key)));
  }
}