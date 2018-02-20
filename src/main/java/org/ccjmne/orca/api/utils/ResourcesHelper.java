package org.ccjmne.orca.api.utils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.jooq.Field;
import org.jooq.Record;
import org.jooq.RecordMapper;
import org.jooq.impl.DSL;
import org.jooq.util.postgres.PostgresDSL;

public class ResourcesHelper {

	/**
	 * Coerces the given <code>value</code> to a either a {@link Boolean} or a
	 * {@link String}, depending on the <code>type</code> specified in
	 * accordance with the database constants
	 * {@link Constants#TAGS_TYPE_BOOLEAN} and
	 * {@link Constants#TAGS_TYPE_STRING}.
	 *
	 * @param type
	 *            Either {@link Constants#TAGS_TYPE_BOOLEAN} or
	 *            {@link Constants#TAGS_TYPE_STRING}
	 *
	 * @return The coerced <code>value</code>, either into a {@link Boolean} or
	 *         a {@link String}, depending on the supplied <code>type</code>
	 */
	public static Object tagValueCoercer(final String type, final String value) {
		return Constants.TAGS_TYPE_BOOLEAN.equals(type) ? Boolean.valueOf(value) : value;
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

	public static <T> RecordMapper<Record, Map<T, Object>> getZipMapper(final String key, final String... fields) {
		return getZipMapper(true, key, fields);
	}

	public static <T> RecordMapper<Record, Map<T, Object>> getZipMapper(final Field<T> key, final Field<?>... fields) {
		return getZipMapper(true, key, fields);
	}

	public static <T> RecordMapper<Record, Map<T, Object>> getZipMapper(final boolean ignoreFalsey, final Field<T> key, final Field<?>... fields) {
		return getZipMapper(ignoreFalsey, key.getName(), Arrays.asList(fields).stream().map(Field::getName).toArray(String[]::new));
	}

	public static <T> RecordMapper<Record, Map<T, Object>> getZipMapper(final boolean ignoreFalsey, final String key, final String... fields) {
		return new RecordMapper<Record, Map<T, Object>>() {

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
							.filter(field -> checkTruthy(slicer.get(field)))
							.collect(Collectors.toMap(field -> field, field -> slicer.get(field))));
				}

				return res;
			}
		};
	}

	@SafeVarargs
	public static <K, V> RecordMapper<Record, Map<K, V>> getSelectMapper(final Field<K> key, final Field<V>... fields) {
		return getSelectMapper((r, x) -> x, key, fields);
	}

	@SuppressWarnings("unchecked")
	public static <K, V> RecordMapper<Record, Map<K, V>> getSelectMapper(final String key, final String... fields) {
		return getSelectMapper((r, x) -> (V) x, key, fields);
	}

	@SafeVarargs
	public static <K, I, V> RecordMapper<Record, Map<K, V>> getSelectMapper(
																			final BiFunction<RecordSlicer, ? super I, ? extends V> coercer,
																			final Field<K> key,
																			final Field<I>... fields) {
		return getSelectMapper(coercer, key.getName(), Arrays.asList(fields).stream().map(Field::getName).toArray(String[]::new));
	}

	public static <K, I, V> RecordMapper<Record, Map<K, V>> getSelectMapper(
																			final BiFunction<RecordSlicer, ? super I, ? extends V> coercer,
																			final String key,
																			final String... fields) {
		return record -> {
			final Map<K, V> res = new HashMap<>();
			@SuppressWarnings("unchecked")
			final K[] keys = (K[]) record.get(key);
			for (int i = 0; i < keys.length; i++) {
				if (keys[i] == null) {
					continue;
				}

				final RecordSlicer slicer = new RecordSlicer(record, i);
				final Optional<? extends V> value = Arrays.asList(fields).stream()
						.map(field -> slicer.<I> get(field))
						.map(x -> coercer.apply(slicer, x))
						.filter(Objects::nonNull)
						.findFirst();

				if (value.isPresent()) {
					res.put(keys[i], value.get());
				}
			}

			return res;
		};
	}

	public static class RecordSlicer {

		private final Record record;
		private final int idx;

		/* package */ RecordSlicer(final Record record, final int idx) {
			this.record = record;
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
	}
}
