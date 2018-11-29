package org.ccjmne.orca.api.utils;

import java.io.IOException;

import org.jooq.Converter;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.TableLike;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;

/**
 * A collection of jOOQ fields and query parts that leverage PostgreSQL' JSON
 * type.
 *
 * @author ccjmne
 */
public class JSONFields {

  public static final DataType<JsonNode> JSON_TYPE = SQLDataType.VARCHAR.asConvertedDataType(new PostgresJSONJacksonJsonNodeConverter());

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
   *         {@link JSONFields#rowToJson(Field...)}
   */
  public static Field<JsonNode> objectAgg(final Field<?> key, final Field<?>... fields) {
    return DSL.coalesce(DSL.field("jsonb_object_agg({0}, ({1})) FILTER (WHERE {0} IS NOT NULL)", JSON_TYPE, key, JSONFields.rowToJson(fields)),
                        JsonNodeFactory.instance.objectNode());
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
    return DSL.coalesce(DSL.field("jsonb_object_agg({0}, {1}) FILTER (WHERE {0} IS NOT NULL)", JSON_TYPE, key, value), JsonNodeFactory.instance.objectNode());
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
    return DSL.coalesce(DSL.field("jsonb_agg({0}) FILTER (WHERE {0} IS NOT NULL)", JSON_TYPE, field), JsonNodeFactory.instance.arrayNode());
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
   *         {@link JSONFields#rowToJson(Field...)}
   */
  public static Field<JsonNode> arrayAgg(final Field<?>... fields) {
    return DSL.coalesce(DSL.field("jsonb_agg({0}) FILTER (WHERE {0} IS NOT NULL)", JSON_TYPE, JSONFields.rowToJson(fields)),
                        JsonNodeFactory.instance.arrayNode());
  }

  /**
   * Builds a {@link JsonNode} from the {@code fields} argument.
   *
   * @param fields
   *          The fields to include in the resulting JSON
   * @return A {@code Field<JsonNode>} built from the specified {@code fields}
   */
  public static Field<JsonNode> rowToJson(final Field<?>... fields) {
    final TableLike<?> inner = DSL.select(fields).asTable();
    return DSL.select(DSL.field("row_to_json({0})", JSON_TYPE, inner)).from(inner).where(fields[0].isNotNull()).asField();
  }

  /**
   * Parses a {@link Field} as JSON.
   */
  public static final Field<JsonNode> toJson(final Field<?> field) {
    return DSL.field("to_jsonb({0})", JSON_TYPE, field);
  }

  @SuppressWarnings("serial")
  private static class PostgresJSONJacksonJsonNodeConverter implements Converter<Object, JsonNode> {

    private final ObjectMapper mapper;

    protected PostgresJSONJacksonJsonNodeConverter() {
      this.mapper = new ObjectMapper(); // TODO: Use CustomObjectMapper?
    }

    @Override
    public JsonNode from(final Object t) {
      try {
        return t == null ? NullNode.instance : this.mapper.readTree(t.toString());
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Object to(final JsonNode u) {
      try {
        return (u == null) || u.equals(NullNode.instance) ? null : this.mapper.writeValueAsString(u);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Class<Object> fromType() {
      return Object.class;
    }

    @Override
    public Class<JsonNode> toType() {
      return JsonNode.class;
    }
  }
}
