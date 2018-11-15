package org.ccjmne.orca.api.utils;

import static org.ccjmne.orca.jooq.classes.Tables.SITES_TAGS;
import static org.ccjmne.orca.jooq.classes.Tables.TAGS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ObjectUtils;
import org.jooq.Converter;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.Record2;
import org.jooq.RecordMapper;
import org.jooq.Table;
import org.jooq.TableLike;
import org.jooq.Transaction;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.util.postgres.PostgresDSL;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.collect.ImmutableList;

// TODO: Rename to SubQueries or something
public class ResourcesHelper {

	public static final DataType<JsonNode> JSON_TYPE = SQLDataType.VARCHAR.asConvertedDataType(new PostgresJSONJacksonJsonNodeConverter());

	/**
	 * Used to determine which parameters are to be considered as tags. Matches
	 * unsigned Base10 integer values.<br />
	 * Pattern: <code>^\d+$</code>
	 */
	public static final Predicate<String> IS_TAG_KEY = Pattern.compile("^\\d+$").asPredicate();

	/**
	 * Coerces the given <code>value</code> to a either a {@link Boolean} or a
	 * {@link String}, depending on the <code>type</code> specified in
	 * accordance with the database constants
	 * {@link Constants#TAGS_TYPE_BOOLEAN} and
	 * {@link Constants#TAGS_TYPE_STRING}.
	 *
	 * @param value
	 *            The {@link String} literal to be coerced
	 * @param type
	 *            Either {@link Constants#TAGS_TYPE_BOOLEAN} or
	 *            {@link Constants#TAGS_TYPE_STRING}
	 *
	 * @return The coerced <code>value</code>, either into a {@link Boolean} or
	 *         a {@link String}, depending on the supplied <code>type</code>
	 */
	// TODO: Delete when rewriting common-resources modules
	public static Object coerceTagValue(final String value, final String type) {
		return Constants.TAGS_TYPE_BOOLEAN.equals(type) ? Boolean.valueOf(value) : value;
	}

	/**
	 * The {@link SITES_TAGS#SITA_VALUE} field coerced to either a boolean or a
	 * string JSON element.
	 */
	public static final Field<JsonNode> TAG_VALUE_COERCED = DSL
			.when(TAGS.TAGS_TYPE.eq(Constants.TAGS_TYPE_BOOLEAN), ResourcesHelper.toJsonb(DSL.cast(SITES_TAGS.SITA_VALUE, Boolean.class)))
			.otherwise(ResourcesHelper.toJsonb(SITES_TAGS.SITA_VALUE));

	/**
	 * Formats a date-type {@link Field} as a {@link String}. Handles the case
	 * where the date is "never" (for certificate expiration, for instance).
	 *
	 * @param field
	 *            The {@code Field<java.sql.Date>} to be formatted
	 * @return A new {@code VARCHAR}-type {@code Field}, formatted as our API
	 *         formats {@link Date}s.
	 */
	public static Field<String> formatDate(final Field<java.sql.Date> field) {
		return DSL
				.when(field.eq(DSL.inline(Constants.DATE_INFINITY)), Constants.DATE_INFINITY_LITERAL)
				.otherwise(DSL.field("to_char({0}, {1})", String.class, field, APIDateFormat.FORMAT));
	}

	/**
	 * Builds a {@link JsonNode} for each {@link Record} from the {@code fields}
	 * argument, and <strong>aggregates</strong> these into a single
	 * {@link JsonNode}, keyed by the {@code key} argument.<br />
	 * <br />
	 * This method <strong>coalesces</strong> the result into an <strong>empty
	 * {@link JsonNode}</strong> when the @c{@code key} field is {@code null}
	 * for an aggregation window.
	 *
	 * @param key
	 *            The field by which the resulting {@code JsonNode} is to be
	 *            keyed
	 * @param fields
	 *            The fields to be included in the resulting {@code JsonNode}
	 *            for each {@code Record}
	 * @return A {@code Field<JsonNode>} built from aggregating the
	 *         {@code fields} argument with
	 *         {@link ResourcesHelper#rowToJson(Field...)}
	 */
	public static Field<JsonNode> jsonbObjectAggNullSafe(final Field<?> key, final Field<?>... fields) {
		return DSL.field(	"COALESCE(jsonb_object_agg({0}, ({1})) FILTER (WHERE {0} IS NOT NULL), '{}')::jsonb", JSON_TYPE, key,
							ResourcesHelper.rowToJson(fields));
	}

	/**
	 * Builds a {@link JsonNode} for each {@link Record} from the {@code fields}
	 * argument, and <strong>aggregates</strong> these into a single
	 * {@link JsonNode}, keyed by the {@code key} argument.<br />
	 * <br />
	 * This method requires {@code key} to not be {@code null} in any
	 * aggregation window. If you want a null-safe version of this (for
	 * aggregating <code>OUTER JOIN</code>s for example), use
	 * {@link ResourcesHelper#jsonbObjectAggNullSafe(Field, Field...)} instead.
	 *
	 * @param key
	 *            The field by which the resulting {@code JsonNode} is to be
	 *            keyed
	 * @param fields
	 *            The fields to be included in the resulting {@code JsonNode}
	 *            for each {@code Record}
	 * @return A {@code Field<JsonNode>} built from aggregating the
	 *         {@code fields} argument with
	 *         {@link ResourcesHelper#rowToJson(Field...)}
	 */
	public static Field<JsonNode> jsonbObjectAgg(final Field<?> key, final Field<?>... fields) {
		return DSL.field("jsonb_object_agg({0}, ({1}))::jsonb", JSON_TYPE, key, ResourcesHelper.rowToJson(fields));
	}

	// TODO: Document
	public static Field<JsonNode> jsonbObjectAggNullSafe(final Field<?> key, final Field<?> value) {
		return DSL.coalesce(DSL.field("jsonb_object_agg({0}, {1})", JSON_TYPE, key, value), JsonNodeFactory.instance.objectNode());
	}

	/**
	 * Builds a {@link JsonNode} from the {@code fields} argument.
	 *
	 * @param fields
	 *            The fields to include in the resulting JSON
	 * @return A {@code Field<JsonNode>} built from the specified {@code fields}
	 */
	public static Field<JsonNode> rowToJson(final Field<?>... fields) {
		final TableLike<?> inner = DSL.select(fields).asTable();
		return DSL.select(DSL.field("row_to_json({0})::jsonb", JSON_TYPE, inner)).from(inner).asField();
	}

	/**
	 * Parses a {@link Field} as JSON.
	 */
	private static final Field<JsonNode> toJsonb(final Field<?> field) {
		return DSL.field("to_jsonb({0})", JSON_TYPE, field);
	}

	/**
	 * Delegates to {@link DSL#arrayAgg(Field)} and gives the resulting
	 * aggregation the specified {@link Field}'s name.
	 *
	 * @param field
	 *            The field to aggregate
	 */
	public static <T> Field<T[]> arrayAgg(final Field<T> field) {
		return DSL.arrayAgg(field).as(field);
	}

	public static Field<String> unaccent(final Field<String> field) {
		return DSL.function("unaccent", String.class, field);
	}

	/**
	 * Delegates to {@link DSL#coalesce(Field, Object)} and gives the resulting
	 * coalition the specified {@link Field}'s name.
	 *
	 * @param field
	 *            The field to coalesce
	 */
	public static <T> Field<T> coalesce(final Field<T> field, final T value) {
		return DSL.coalesce(field, value).as(field);
	}

	/**
	 * Aggregate function on the supplied {@link Field} that selects
	 * <strong>distinct</strong> values into an {@link Array} while omitting
	 * <code>null</code> entries.
	 *
	 * @param field
	 *            The field to aggregate
	 */
	public static <T> Field<T[]> arrayAggDistinctOmitNull(final Field<T> field) {
		return PostgresDSL.arrayRemove(DSL.arrayAggDistinct(field), DSL.castNull(field.getType())).as(field);
	}

	/**
	 * Generates a {@link Query} that will remove duplicates and reintroduce
	 * missing entries in the virtual sequence of ordering values stored as a
	 * {@link Table}'s {@link Field}.
	 *
	 * @param table
	 *            The {@link Table} whose ordering field to cleanup
	 * @param key
	 *            A {@link Field} acting as unique ID for the records in this
	 *            table (most likely its <em>primary key</em>)
	 * @param order
	 *            The {@link Field} by which this table's records are to be
	 *            ordered
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

	public static <K, V> RecordMapper<Record, Map<String, Object>> getMapperWithZip(
																					final ZipRecordMapper<Record, Map<K, V>> zipMapper,
																					final String zipAs) {
		return record -> {
			final Map<String, Object> res = new HashMap<>();
			final List<String> fields = new ArrayList<>(Arrays.stream(record.fields()).map(Field::getName).collect(Collectors.toList()));
			fields.removeAll(zipMapper.getZippedFields());
			fields.forEach(field -> res.put(field, record.get(field)));
			final Map<K, V> map = zipMapper.map(record);
			if (!map.isEmpty()) {
				res.put(zipAs, map);
			}

			return res;
		};
	}

	public static <T> ZipRecordMapper<Record, Map<T, Object>> getZipMapper(final String key, final String... fields) {
		return ResourcesHelper.getZipMapper(true, key, fields);
	}

	public static <T> ZipRecordMapper<Record, Map<T, Object>> getZipMapper(final Field<T> key, final Field<?>... fields) {
		return ResourcesHelper.getZipMapper(true, key, fields);
	}

	public static <T> ZipRecordMapper<Record, Map<T, Object>> getZipMapper(final boolean ignoreFalsey, final Field<T> key, final Field<?>... fields) {
		return ResourcesHelper.getZipMapper(ignoreFalsey, key.getName(), Arrays.asList(fields).stream().map(Field::getName).toArray(String[]::new));
	}

	public static <T> ZipRecordMapper<Record, Map<T, Object>> getZipMapper(final boolean ignoreFalsey, final String key, final String... fields) {
		return new ZipRecordMapper<Record, Map<T, Object>>(ImmutableList.<String> builder().addAll(Arrays.asList(fields)).add(key).build()) {

			/**
			 * Passing this method to {@link Stream#filter} would discard all
			 * <code>null</code> entries.<br />
			 * Additionally, if <code>ignoreFalsey</code> is set, also discard
			 * all the following:
			 * <ul>
			 * <li><code>Boolean.FALSE</code></li>
			 * <li><code>Integer.valueOf(0)</code></li>
			 * <li><code>""</code> (the empty <code>String</code>)</li>
			 * </ul>
			 */
			private final boolean checkTruthy(final Object o) {
				return ignoreFalsey	? (null != o) && !Boolean.FALSE.equals(o) && !Integer.valueOf(0).equals(o) && !"".equals(o)
									: null != o;
			}

			@Override
			@SuppressWarnings("unchecked")
			public Map<T, Object> map(final Record record) {
				final Map<T, Object> res = new HashMap<>();
				final T[] keys = (T[]) record.get(key);
				for (int i = 0; i < keys.length; i++) {
					if (keys[i] == null) {
						continue;
					}

					final RecordSlicer slicer = new RecordSlicer(record, i);
					res.put(keys[i], Arrays.asList(fields).stream()
							.filter(field -> this.checkTruthy(slicer.get(field)))
							.collect(Collectors.toMap(field -> field, field -> slicer.get(field))));
				}

				return res;
			}
		};
	}

	@SafeVarargs
	public static <K, V> ZipRecordMapper<Record, Map<K, V>> getZipSelectMapper(final Field<K[]> key, final Field<V[]>... fields) {
		return ResourcesHelper.getZipSelectMapper((r, x) -> x, key, fields);
	}

	@SuppressWarnings("unchecked")
	public static <K, V> ZipRecordMapper<Record, Map<K, V>> getZipSelectMapper(final String key, final String... fields) {
		return ResourcesHelper.getZipSelectMapper((r, x) -> (V) x, key, fields);
	}

	@SafeVarargs
	public static <K, I, V> ZipRecordMapper<Record, Map<K, V>> getZipSelectMapper(
																					final BiFunction<? super RecordSlicer, ? super I, ? extends V> coercer,
																					final Field<K[]> key,
																					final Field<I[]>... fields) {
		return ResourcesHelper.getZipSelectMapper(coercer, key.getName(), Arrays.asList(fields).stream().map(Field::getName).toArray(String[]::new));
	}

	public static <K, I, V> ZipRecordMapper<Record, Map<K, V>> getZipSelectMapper(
																					final BiFunction<? super RecordSlicer, ? super I, ? extends V> coercer,
																					final String key,
																					final String... fields) {
		return new ZipRecordMapper<Record, Map<K, V>>(ImmutableList.<String> builder().addAll(Arrays.asList(fields)).add(key).build()) {

			@Override
			@SuppressWarnings("unchecked")
			public Map<K, V> map(final Record record) {
				final Map<K, V> res = new HashMap<>();
				final K[] keys = (K[]) ObjectUtils.defaultIfNull(record.get(key), new Object[] {});
				for (int i = 0; i < keys.length; i++) {
					if (keys[i] == null) {
						continue;
					}

					final RecordSlicer slicer = new RecordSlicer(record, i);
					final Optional<? extends V> value = Arrays.asList(fields).stream()
							.map(field -> slicer.<I> get(field))
							.map(slice -> coercer.apply(slicer, slice))
							.filter(Objects::nonNull)
							.findFirst();

					if (value.isPresent()) {
						res.put(keys[i], value.get());
					}
				}

				return res;
			}
		};
	}

	public static FieldsCoercer getBiFieldCoercer(
													final Field<?> coerce,
													final Field<?> using,
													final BiFunction<? super Object, ? super Object, ? extends Object> coercer) {
		return new FieldsCoercer(Arrays.asList(using).stream().map(Field::getName).collect(Collectors.toList())) {

			@Override
			public void coerceWithin(final Map<String, Object> source) {
				source.computeIfPresent(coerce.getName(), (unused, needsCoercing) -> coercer.apply(needsCoercing, source.get(using.getName())));
			}
		};
	}

	public static <V, U> FieldsCoercer getBiFieldSlicingCoercer(
																final Field<V> coerce,
																final Field<U> using,
																final BiFunction<? super RecordSlicer, ? super V, ? extends Object> coercer) {
		return new FieldsCoercer(Arrays.asList(using).stream().map(Field::getName).collect(Collectors.toList())) {

			@Override
			@SuppressWarnings("unchecked")
			public void coerceWithin(final Map<String, Object> source) {
				source.computeIfPresent(coerce.getName(), (unused, needsCoercing) -> {
					final AtomicInteger i = new AtomicInteger();

					return Arrays
							.stream(needsCoercing instanceof Object[] ? (Object[]) needsCoercing : new Object[] { (needsCoercing) })
							.map(value -> coercer.apply(new RecordSlicer(source, i.getAndIncrement()), (V) value)).collect(Collectors.toList());
				});
			}
		};
	}

	public static RecordMapper<Record, Map<String, Object>> getCoercerMapper(final FieldsCoercer... coercers) {
		return ResourcesHelper.coercing(record -> record.intoMap(), coercers);
	}

	public static RecordMapper<Record, Map<String, Object>> coercing(final RecordMapper<Record, Map<String, Object>> mapper, final FieldsCoercer... coercers) {
		return record -> ResourcesHelper.apply(mapper.map(record), coercers);
	}

	public static Map<String, Object> apply(final Map<String, Object> map, final FieldsCoercer... coercers) {
		Arrays.stream(coercers).peek(c -> c.coerceWithin(map)).forEach(c -> c.getConsumedFields().forEach(map::remove));
		return map;
	}

	public static abstract class FieldsCoercer {

		private final Collection<String> consumedFields;

		public FieldsCoercer(final Collection<String> consumedFields) {
			this.consumedFields = consumedFields;
		}

		public abstract void coerceWithin(final Map<String, Object> source);

		public Collection<String> getConsumedFields() {
			return this.consumedFields;
		}

		public final FieldsCoercer nonConsuming() {
			return new NonConsumingFieldsCoercer(this);
		}

		private class NonConsumingFieldsCoercer extends FieldsCoercer {

			private final FieldsCoercer deletage;

			protected NonConsumingFieldsCoercer(final FieldsCoercer deletage) {
				super(Collections.EMPTY_LIST);
				this.deletage = deletage;
			}

			@Override
			public void coerceWithin(final Map<String, Object> source) {
				this.deletage.coerceWithin(source);
			}
		}
	}

	public static class RecordSlicer {

		private final Map<String, Object> record;
		private final int idx;

		protected RecordSlicer(final Record record, final int idx) {
			this(record.intoMap(), idx);
		}

		protected RecordSlicer(final Map<String, Object> map, final int idx) {
			this.record = map;
			this.idx = idx;
		}

		public final <T> T get(final Field<T[]> field) {
			return this.get(field.getName());
		}

		@SuppressWarnings("unchecked")
		public final <T> T get(final String field) {
			try {
				return ((T[]) this.record.get(field))[this.idx];
			} catch (final IndexOutOfBoundsException e) {
				throw new IllegalArgumentException(String.format("This slicer could not operate on field: %s", field));
			}
		}

		public final <T> T getRaw(final Field<T> field) {
			return this.getRaw(field.getName());
		}

		@SuppressWarnings("unchecked")
		public final <T> T getRaw(final String field) {
			return (T) this.record.get(field);
		}
	}

	private abstract static class ZipRecordMapper<R extends Record, E> implements RecordMapper<R, E> {

		private final Collection<String> zippedFields;

		protected ZipRecordMapper(final Collection<String> zippedFields) {
			this.zippedFields = zippedFields;
		}

		public Collection<String> getZippedFields() {
			return this.zippedFields;
		}
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
