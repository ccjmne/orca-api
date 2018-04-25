package org.ccjmne.orca.api.utils;

import static org.ccjmne.orca.jooq.classes.Tables.SITES_TAGS;
import static org.ccjmne.orca.jooq.classes.Tables.TAGS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.ws.rs.core.UriInfo;

import org.jooq.Field;
import org.jooq.Record;
import org.jooq.RecordMapper;
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
	 * @param type
	 *            Either {@link Constants#TAGS_TYPE_BOOLEAN} or
	 *            {@link Constants#TAGS_TYPE_STRING}
	 *
	 * @return The coerced <code>value</code>, either into a {@link Boolean} or
	 *         a {@link String}, depending on the supplied <code>type</code>
	 */
	public static Object coerceTagValue(final String type, final String value) {
		return Constants.TAGS_TYPE_BOOLEAN.equals(type) ? Boolean.valueOf(value) : value;
	}

	public static final FieldsCoercer TAG_VALUE_SCOERCER = ResourcesHelper
			.getBiFieldCoercer(	TAGS.TAGS_TYPE, SITES_TAGS.SITA_VALUE,
								(BiFunction<? super Object, ? super Object, ? extends Object>) (type, value) -> ResourcesHelper
										.coerceTagValue((String) type, (String) value));

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

	private abstract static class ZipRecordMapper<R extends Record, E> implements RecordMapper<R, E> {

		private final Collection<String> zippedFields;

		/* package */ ZipRecordMapper(final Collection<String> zippedFields) {
			this.zippedFields = zippedFields;
		}

		public Collection<String> getZippedFields() {
			return this.zippedFields;
		}
	}

	public static RecordMapper<Record, Map<String, Object>> getMapperWithZip(
																				final ZipRecordMapper<Record, Map<Integer, Object>> zipMapper,
																				final String zipAs) {
		return record -> {
			final Map<String, Object> res = new HashMap<>();
			final List<String> fields = new ArrayList<>(Arrays.stream(record.fields()).map(Field::getName).collect(Collectors.toList()));
			fields.removeAll(zipMapper.getZippedFields());
			fields.forEach(field -> res.put(field, record.get(field)));
			final Map<Integer, Object> map = zipMapper.map(record);
			if (!map.isEmpty()) {
				res.put(zipAs, map);
			}

			return res;
		};
	}

	public static <T> ZipRecordMapper<Record, Map<T, Object>> getZipMapper(final String key, final String... fields) {
		return getZipMapper(true, key, fields);
	}

	public static <T> ZipRecordMapper<Record, Map<T, Object>> getZipMapper(final Field<T> key, final Field<?>... fields) {
		return getZipMapper(true, key, fields);
	}

	public static <T> ZipRecordMapper<Record, Map<T, Object>> getZipMapper(final boolean ignoreFalsey, final Field<T> key, final Field<?>... fields) {
		return getZipMapper(ignoreFalsey, key.getName(), Arrays.asList(fields).stream().map(Field::getName).toArray(String[]::new));
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
							.filter(field -> checkTruthy(slicer.get(field)))
							.collect(Collectors.toMap(field -> field, field -> slicer.get(field))));
				}

				return res;
			}
		};
	}

	@SafeVarargs
	public static <K, V> RecordMapper<Record, Map<K, V>> getZipSelectMapper(final Field<K> key, final Field<V>... fields) {
		return getZipSelectMapper((r, x) -> x, key, fields);
	}

	@SuppressWarnings("unchecked")
	public static <K, V> RecordMapper<Record, Map<K, V>> getZipSelectMapper(final String key, final String... fields) {
		return getZipSelectMapper((r, x) -> (V) x, key, fields);
	}

	@SafeVarargs
	public static <K, I, V> ZipRecordMapper<Record, Map<K, V>> getZipSelectMapper(
																					final BiFunction<RecordSlicer, ? super I, ? extends V> coercer,
																					final Field<K> key,
																					final Field<I>... fields) {
		return getZipSelectMapper(coercer, key.getName(), Arrays.asList(fields).stream().map(Field::getName).toArray(String[]::new));
	}

	public static <K, I, V> ZipRecordMapper<Record, Map<K, V>> getZipSelectMapper(
																					final BiFunction<RecordSlicer, ? super I, ? extends V> coercer,
																					final String key,
																					final String... fields) {
		return new ZipRecordMapper<Record, Map<K, V>>(ImmutableList.<String> builder().addAll(Arrays.asList(fields)).add(key).build()) {

			@Override
			@SuppressWarnings("unchecked")
			public Map<K, V> map(final Record record) {
				final Map<K, V> res = new HashMap<>();
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
			}
		};
	}

	public static FieldsCoercer getBiFieldCoercer(
													final Field<?> typeField,
													final Field<?> valueField,
													final BiFunction<? super Object, ? super Object, ? extends Object> coercer) {
		return new FieldsCoercer(Arrays.asList(typeField).stream().map(Field::getName).collect(Collectors.toList())) {

			@Override
			public void coerceWithin(final Map<String, Object> source) {
				source.computeIfPresent(valueField.getName(), (unused, value) -> coercer.apply(source.get(typeField.getName()), value));
			}
		};
	}

	public static RecordMapper<Record, Map<String, Object>> getCoercerMapper(final FieldsCoercer... coercers) {
		return record -> {
			final Map<String, Object> res = record.intoMap();
			Arrays.stream(coercers).peek(c -> c.coerceWithin(res)).forEach(c -> c.getConsumedFields().forEach(res::remove));
			return res;
		};
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
