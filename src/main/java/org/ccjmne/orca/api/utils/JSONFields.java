package org.ccjmne.orca.api.utils;

import org.ccjmne.orca.jooq.PostgresJSONJacksonJsonNodeConverter;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.TableLike;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * A collection of jOOQ fields and query parts that leverage PostgreSQL' JSON
 * type.
 *
 * @author ccjmne
 */
public class JSONFields {

  public static final DataType<JsonNode> JSON_TYPE = SQLDataType.VARCHAR.asConvertedDataType(new PostgresJSONJacksonJsonNodeConverter());

  /**
   * Get JSON object field by key (operator: {@code ->}).
   * 
   * @param field
   *          The {@code json} (or {@code jsonb} field
   * @param key
   *          The key to select the value of
   * @return A new {@code Field<JsonNode>} that uses the {@code ->} SQL json(b)
   *         operator to select a JSON object field by key
   */
  public static Field<JsonNode> getByKey(final Field<JsonNode> field, final String key) {
    return DSL.field("COALESCE({0} -> {1}, '{}'::jsonb)", JSON_TYPE, field, key);
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
   * @return A new {@code Field<JsonNode>} with the updated key-value pair
   */
  public static Field<JsonNode> setByKey(Field<JsonNode> field, final String key, final JsonNode value) {
    return DSL.field("jsonb_set({0}, {1}, {2})", JSON_TYPE, field, DSL.array(key), DSL.val(value, JSON_TYPE));
  }

  /**
   * Delete JSON object value at key (operator: {@code #-}).
   * 
   * @param field
   *          The JSON field to be updated
   * @param key
   *          The key to be deleted from the specified {@code field}
   * @return A new {@code Field<JsonNode>} stripped of the specified key
   */
  public static Field<JsonNode> deleteByKey(Field<JsonNode> field, final String key) {
    return DSL.field("{0} #- {1}", JSON_TYPE, field, DSL.array(key));
  }

  /**
   * Builds a {@link JsonNode} for each {@link Record} from the {@code fields}
   * argument, and <strong>aggregates</strong> these into a single
   * {@link JsonNode}, keyed by the {@code key} argument.<br />
   * <br />
   * This method <strong>coalesces</strong> the result into an <strong>empty
   * {@link JsonNode}</strong> when the {@code key} field is {@code null}
   * for an aggregation window.
   *
   * @param key
   *          The field by which the resulting {@code JsonNode} is to be
   *          keyed
   * @param fields
   *          The fields to be included in the resulting {@code JsonNode}
   *          for each {@code Record}
   * @return A {@code Field<JsonNode>} built from aggregating the
   *         {@code fields} argument with
   *         {@link JSONFields#toJson(Field...)}
   */
  public static Field<JsonNode> objectAgg(final Field<?> key, final Field<?>... fields) {
    return DSL.field("COALESCE(jsonb_object_agg({0}, ({1})) FILTER (WHERE {0} IS NOT NULL), '{}'::jsonb)", JSON_TYPE, key, JSONFields.toJson(fields));
  }

  /**
   * Aggregates the {@link Record}s into a {@link JsonNode}, as a one-to-one
   * mapping of the {@code value} argument, by the {@code key} field.<br />
   * <br />
   * This method <strong>coalesces</strong> the result into an <strong>empty
   * {@link JsonNode}</strong> when the {@code key} field is {@code null}
   * for an aggregation window.
   *
   * @param key
   *          The field by which the resulting {@code JsonNode} is to be
   *          keyed
   * @param value
   *          The field to be included in the resulting {@code JsonNode}
   *          for each {@code Record}
   * @return A one-to-one {@code Field<JsonNode>} mapping {@code key} to
   *         {@code value}
   */
  public static Field<JsonNode> objectAgg(final Field<?> key, final Field<?> value) {
    return DSL.field("COALESCE(jsonb_object_agg({0}, {1}) FILTER (WHERE {0} IS NOT NULL), '{}'::jsonb)", JSON_TYPE, key, value);
  }

  /**
   * <strong>Aggregates</strong> the {@code field} argument into a single JSON
   * Array.<br />
   * <br />
   * This method <strong>filters out</strong> {@code null} values.
   *
   * @param field
   *          The field to be aggregated
   * @return A {@code Field<JsonNode>} built from aggregating the
   *         {@code field} argument
   */
  public static Field<JsonNode> arrayAgg(final Field<?> field) {
    return DSL.field("COALESCE(jsonb_agg({0}) FILTER (WHERE {0} IS NOT NULL), '[]'::jsonb)", JSON_TYPE, field);
  }

  /**
   * Builds a {@link JsonNode} for each {@link Record} from the {@code fields}
   * argument, and <strong>aggregates</strong> these into a single JSON
   * Array.<br />
   *
   * @param fields
   *          The fields to be included in the resulting {@code JsonNode}
   *          for each {@code Record}
   * @return A {@code Field<JsonNode>} built from aggregating the
   *         {@code fields} argument with
   *         {@link JSONFields#toJson(Field...)}
   */
  public static Field<JsonNode> arrayAgg(final Field<?>... fields) {
    return DSL.field("COALESCE(jsonb_agg({0}) FILTER (WHERE {0} IS NOT NULL), '[]'::jsonb)", JSON_TYPE, JSONFields.toJson(fields));
  }

  /**
   * Builds a {@link JsonNode} from the {@code fields} argument.
   *
   * @param fields
   *          The fields to include in the resulting JSON
   * @return A {@code Field<JsonNode>} built from the specified {@code fields}
   */
  public static Field<JsonNode> toJson(final Field<?>... fields) {
    final TableLike<?> inner = DSL.select(fields).asTable();
    return DSL.select(DSL.field("to_jsonb({0})", JSON_TYPE, inner)).from(inner).where(fields[0].isNotNull()).asField();
  }

  /**
   * Parses a {@link Field} as JSON.
   */
  public static final Field<JsonNode> toJson(final Field<?> field) {
    return DSL.field("to_jsonb({0})", JSON_TYPE, field);
  }
}
