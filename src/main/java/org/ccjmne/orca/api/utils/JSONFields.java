package org.ccjmne.orca.api.utils;

import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.TableLike;
import org.jooq.impl.DSL;

/**
 * A collection of jOOQ fields and query parts that leverage PostgreSQL' JSON
 * type.
 *
 * @author ccjmne
 */
public class JSONFields {

  /**
   * Get JSON object field by key (operator: {@code ->}).
   *
   * @param field
   *          The {@code json} (or {@code jsonb} field
   * @param key
   *          The key to select the value of
   * @return A new {@code Field<JSONB>} that uses the {@code ->} SQL json(b)
   *         operator to select a JSON object field by key
   */
  public static Field<JSONB> getByKey(final Field<JSONB> field, final String key) {
    return DSL.field("{0} -> {1}", JSONB.class, field, key);
  }

  /**
   * Set JSON object value at key (function: {@code jsonb_set}).
   *
   * @param field
   *          The JSON field to be updated
   * @param key
   *          The key to be assigned the specified {@code value}
   * @param value
   *          The value to set for the given {@code key}
   * @return A new {@code Field<JSONB>} with the updated key-value pair
   */
  public static Field<JSONB> setByKey(final Field<JSONB> field, final String key, final JSONB value) {
    return DSL.field("jsonb_set({0}, {1}, {2})", JSONB.class, field, DSL.array(key), value);
  }

  /**
   * Delete JSON object value at key (operator: {@code #-}).
   *
   * @param field
   *          The JSON field to be updated
   * @param key
   *          The key to be deleted from the specified {@code field}
   * @return A new {@code Field<JSONB>} stripped of the specified key
   */
  public static Field<JSONB> deleteByKey(final Field<JSONB> field, final String key) {
    return DSL.field("{0} #- {1}", JSONB.class, field, DSL.array(key));
  }

  /**
   * Builds a {@link JSONB} for each {@link Record} from the {@code fields}
   * argument, and <strong>aggregates</strong> these into a single
   * {@link JSONB}, keyed by the {@code key} argument.<br />
   * <br />
   * This method <strong>coalesces</strong> the result into an <strong>empty
   * {@link JSONB}</strong> when the {@code key} field is {@code null}
   * for an aggregation window.
   *
   * @param key
   *          The field by which the resulting {@code JSONB} is to be
   *          keyed
   * @param fields
   *          The fields to be included in the resulting {@code JSONB}
   *          for each {@code Record}
   * @return A {@code Field<JSONB>} built from aggregating the
   *         {@code fields} argument with
   *         {@link JSONFields#toJson(Field...)}
   */
  public static Field<JSONB> objectAgg(final Field<?> key, final Field<?>... fields) {
    return DSL.field("COALESCE(jsonb_object_agg({0}, ({1})) FILTER (WHERE {0} IS NOT NULL), '{}'::jsonb)", JSONB.class, key, JSONFields.toJson(fields));
  }

  /**
   * Aggregates the {@link Record}s into a {@link JSONB}, as a one-to-one
   * mapping of the {@code value} argument, by the {@code key} field.<br />
   * <br />
   * This method <strong>coalesces</strong> the result into an <strong>empty
   * {@link JSONB}</strong> when the {@code key} field is {@code null}
   * for an aggregation window.
   *
   * @param key
   *          The field by which the resulting {@code JSONB} is to be
   *          keyed
   * @param value
   *          The field to be included in the resulting {@code JSONB}
   *          for each {@code Record}
   * @return A one-to-one {@code Field<JSONB>} mapping {@code key} to
   *         {@code value}
   */
  public static Field<JSONB> objectAgg(final Field<?> key, final Field<?> value) {
    return DSL.field("COALESCE(jsonb_object_agg({0}, {1}) FILTER (WHERE {0} IS NOT NULL), '{}'::jsonb)", JSONB.class, key, value);
  }

  /**
   * <strong>Aggregates</strong> the {@code field} argument into a single JSON
   * Array.<br />
   * <br />
   * This method <strong>filters out</strong> {@code null} values.
   *
   * @param field
   *          The field to be aggregated
   * @return A {@code Field<JSONB>} built from aggregating the
   *         {@code field} argument
   */
  public static Field<JSONB> arrayAgg(final Field<?> field) {
    return DSL.field("COALESCE(jsonb_agg({0}) FILTER (WHERE {0} IS NOT NULL), '[]'::jsonb)", JSONB.class, field);
  }

  /**
   * Builds a {@link JSONB} for each {@link Record} from the {@code fields}
   * argument, and <strong>aggregates</strong> these into a single JSON
   * Array.<br />
   *
   * @param fields
   *          The fields to be included in the resulting {@code JSONB}
   *          for each {@code Record}
   * @return A {@code Field<JSONB>} built from aggregating the
   *         {@code fields} argument with
   *         {@link JSONFields#toJson(Field...)}
   */
  public static Field<JSONB> arrayAgg(final Field<?>... fields) {
    return DSL.field("COALESCE(jsonb_agg({0}) FILTER (WHERE {0} IS NOT NULL), '[]'::jsonb)", JSONB.class, JSONFields.toJson(fields));
  }

  /**
   * Builds a {@link JSONB} from the {@code fields} argument.
   *
   * @param fields
   *          The fields to include in the resulting JSON
   * @return A {@code Field<JSONB>} built from the specified {@code fields}
   */
  public static Field<JSONB> toJson(final Field<?>... fields) {
    final TableLike<?> inner = DSL.select(fields).asTable();
    return DSL.select(DSL.field("to_jsonb({0})", JSONB.class, inner)).from(inner).where(fields[0].isNotNull()).asField();
  }

  /**
   * Parses a {@link Field} as JSON.
   */
  public static final Field<JSONB> toJson(final Field<?> field) {
    return DSL.field("to_jsonb({0})", JSONB.class, field);
  }
}
