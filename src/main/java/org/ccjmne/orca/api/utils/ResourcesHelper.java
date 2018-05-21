package org.ccjmne.orca.api.utils;

import static org.ccjmne.orca.jooq.classes.Tables.SITES_TAGS;
import static org.ccjmne.orca.jooq.classes.Tables.TAGS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.ws.rs.core.UriInfo;

import org.apache.commons.lang3.ObjectUtils;
import org.jooq.Field;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.Record2;
import org.jooq.RecordMapper;
import org.jooq.Table;
import org.jooq.Transaction;
import org.jooq.impl.DSL;
import org.jooq.util.postgres.PostgresDSL;

import com.google.common.collect.ImmutableList;

public class ResourcesHelper {

	/**
	 * Used to determine which parameters are to be considered as tags. Matches
	 * unsigned Base10 integer values.<br />
	 * Pattern: <code>^\d+$</code>
	 */
	private static final Predicate<String> IS_TAG_KEY = Pattern.compile("^\\d+$").asPredicate();

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
	public static Object coerceTagValue(final String value, final String type) {
		return Constants.TAGS_TYPE_BOOLEAN.equals(type) ? Boolean.valueOf(value) : value;
	}

	public static final FieldsCoercer TAG_VALUE_COERCER = ResourcesHelper
			.getBiFieldCoercer(SITES_TAGS.SITA_VALUE, TAGS.TAGS_TYPE, (value, type) -> ResourcesHelper.coerceTagValue((String) value, (String) type));

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
	 * Extracts a multi-valued {@link Map} of tags from the query parameters for
	 * a specific API call.<br />
	 * The {@link Predicate} used to determine which parameters are to be
	 * considered as tags is {@link ResourcesHelper#IS_TAG_KEY}
	 *
	 * @param uriInfo
	 *            The {@link UriInfo} representing the request
	 */
	public static Map<Integer, List<String>> getTagsFromUri(final UriInfo uriInfo) {
		return uriInfo.getQueryParameters().entrySet().stream()
				.filter(x -> IS_TAG_KEY.test(x.getKey()))
				.collect(Collectors.<Entry<String, List<String>>, Integer, List<String>> toMap(x -> Integer.valueOf(x.getKey()), Entry::getValue));
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
	public static <K, V> ZipRecordMapper<Record, Map<K, V>> getZipSelectMapper(final Field<K> key, final Field<V>... fields) {
		return ResourcesHelper.getZipSelectMapper((r, x) -> x, key, fields);
	}

	@SuppressWarnings("unchecked")
	public static <K, V> ZipRecordMapper<Record, Map<K, V>> getZipSelectMapper(final String key, final String... fields) {
		return ResourcesHelper.getZipSelectMapper((r, x) -> (V) x, key, fields);
	}

	@SafeVarargs
	public static <K, I, V> ZipRecordMapper<Record, Map<K, V>> getZipSelectMapper(
																					final BiFunction<? super RecordSlicer, ? super I, ? extends V> coercer,
																					final Field<K> key,
																					final Field<I>... fields) {
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

		/* package */ RecordSlicer(final Record record, final int idx) {
			this(record.intoMap(), idx);
		}

		/* package */ RecordSlicer(final Map<String, Object> map, final int idx) {
			this.record = map;
			this.idx = idx;
		}

		public final <T> T get(final Field<T> field) {
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

		/* package */ ZipRecordMapper(final Collection<String> zippedFields) {
			this.zippedFields = zippedFields;
		}

		public Collection<String> getZippedFields() {
			return this.zippedFields;
		}
	}
}
