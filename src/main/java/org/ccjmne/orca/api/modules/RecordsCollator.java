package org.ccjmne.orca.api.modules;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import org.apache.http.client.utils.URLEncodedUtils;
import org.ccjmne.orca.api.utils.ResourcesHelper;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Select;
import org.jooq.SelectFinalStep;
import org.jooq.SelectQuery;
import org.jooq.SortField;
import org.jooq.impl.DSL;

import com.google.common.base.MoreObjects;

/**
 * Provides filtering, sorting and pagination methods to be applied to any
 * {@link SelectQuery}.<br />
 * Its configuration is automatically obtained from the query parameters of the
 * current {@link HttpServletRequest}.
 *
 * @author ccjmne
 */
public class RecordsCollator {

	private static final String PARAMETER_NAME_PAGE_SIZE = "page-size";
	private static final String PARAMETER_NAME_PAGE_OFFSET = "page-offset";
	private static final Pattern SORTING_ENTRY = Pattern.compile("^sorting\\[(?<field>[^]]+)\\]=(?<order>.*)$");
	private static final Pattern FILTER_ENTRY = Pattern.compile("^filter\\[(?<field>[^]]+)\\]=(?<value>.*)$");

	private final List<? extends Sort> orderBy;
	private final List<? extends Filter> filterWhere;
	private final int limit;
	private final int offset;

	/**
	 * RAII constructor that parses the context's query parameters and creates
	 * the corresponding JOOQ {@link Condition}s and {@link SortField}s.<br />
	 * <br />
	 * The order in which the sorting query parameters appear is preserved using
	 * {@link URLEncodedUtils#parse(java.net.URI, String)}.
	 */
	@Inject
	public RecordsCollator(@Context final UriInfo uriInfo) {
		final String pSize = uriInfo.getQueryParameters().getFirst(PARAMETER_NAME_PAGE_SIZE);
		this.limit = pSize == null ? 0 : Integer.parseInt(pSize);
		this.offset = this.limit * Integer.parseInt(MoreObjects.firstNonNull(uriInfo.getQueryParameters().getFirst(PARAMETER_NAME_PAGE_OFFSET), "0"));
		this.orderBy = URLEncodedUtils.parse(uriInfo.getRequestUri(), "UTF-8").stream().map(p -> String.format("%s=%s", p.getName(), p.getValue()))
				.map(SORTING_ENTRY::matcher)
				.filter(Matcher::matches)
				.map(m -> new Sort(m.group("field"), m.group("order")))
				.collect(Collectors.toList());
		this.filterWhere = uriInfo.getQueryParameters().entrySet().stream().flatMap(e -> e.getValue().stream().map(v -> String.format("%s=%s", e.getKey(), v)))
				.map(FILTER_ENTRY::matcher)
				.filter(Matcher::matches)
				.map(m -> new Filter(m.group("field"), m.group("value")))
				.collect(Collectors.toList());
	}

	public <T extends Record> SelectQuery<T> applyFiltering(final SelectQuery<T> query) {
		query.addConditions(this.filterWhere.stream()
				.map(Filter.toCondition(Arrays.asList(query.fields())))
				.filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList()));
		return query;
	}

	/**
	 * Overload of {@link #applyFiltering(SelectQuery)}.
	 *
	 * @param select
	 *            The {@link Select} whose underlying query to collate
	 * @return The underlying query, for method chaining purposes
	 */
	public <T extends Record> SelectQuery<T> applyFiltering(final SelectFinalStep<T> select) {
		return this.applyFiltering(select.getQuery());
	}

	/**
	 * Ideally <strong>only applied onto the outermost query</strong> that
	 * actually gets returned to the client.<br />
	 * Sorting the results of a sub-query used internally serves no purpose.
	 *
	 * @param query
	 *            The {@link SelectQuery} to which sorting should be applied
	 * @return The original query, for method chaining purposes
	 */
	public <T extends Record> SelectQuery<T> applySorting(final SelectQuery<T> query) {
		query.addOrderBy(this.orderBy.stream()
				.map(Sort.toSortField(Arrays.asList(query.fields())))
				.filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList()));
		return query;
	}

	/**
	 * Overload of {@link #applySorting(SelectQuery)}.
	 *
	 * @param select
	 *            The {@link Select} whose underlying query to collate
	 * @return The underlying query, for method chaining purposes
	 */
	public <T extends Record> SelectQuery<T> applySorting(final SelectFinalStep<T> select) {
		return this.applySorting(select.getQuery());
	}

	/**
	 * Should <strong>only ever be applied onto the outermost query</strong>
	 * that actually gets returned to the client.<br />
	 * Should <strong>never be used on a sub-query </strong>used internally to
	 * refine the results.
	 *
	 * @param query
	 *            The {@link SelectQuery} to which pagination should be applied
	 * @return The original query, for method chaining purposes
	 */
	public <T extends Record> SelectQuery<T> applyPagination(final SelectQuery<T> query) {
		if (this.limit > 0) {
			query.addLimit(this.offset, this.limit);
		}

		return query;
	}

	/**
	 * Overload of {@link #applyPagination(SelectQuery)}.
	 *
	 * @param select
	 *            The {@link Select} whose underlying query to collate
	 * @return The underlying query, for method chaining purposes
	 */
	public <T extends Record> SelectQuery<T> applyPagination(final SelectFinalStep<T> select) {
		return this.applyPagination(select.getQuery());
	}

	/**
	 * Stands for <em>"apply filtering and sorting"</em>.<br />
	 * <br />
	 *
	 * Ideally <strong>only applied onto the outermost query</strong> that
	 * actually gets returned to the client.<br />
	 * Sorting the results of a sub-query used internally serves no purpose (see
	 * {@link #applySorting(SelectQuery)}.
	 *
	 * @param query
	 *            The {@link SelectQuery} to which filtering and sorting should
	 *            be applied
	 * @return The original query, for method chaining purposes
	 */
	public <T extends Record> SelectQuery<T> applyFAndS(final SelectQuery<T> query) {
		return this.applySorting(this.applyFiltering(query));
	}

	/**
	 * Overload of {@link #applyFAndS(SelectQuery)}.
	 *
	 * @param select
	 *            The {@link Select} whose underlying query to collate
	 * @return The underlying query, for method chaining purposes
	 */
	public <T extends Record> SelectQuery<T> applyFAndS(final SelectFinalStep<T> select) {
		return this.applyFAndS(select.getQuery());
	}

	/**
	 * Should <strong>only ever be applied onto the outermost query</strong>
	 * that actually gets returned to the client.<br />
	 * Should <strong>never be used on a sub-query </strong>used internally to
	 * refine the results (see {@link #applyPagination(SelectQuery)}).
	 *
	 * @param query
	 *            The {@link SelectQuery} to which filtering, sorting and
	 *            pagination should be applied
	 * @return The original query, for method chaining purposes
	 */
	public <T extends Record> SelectQuery<T> applyAll(final SelectQuery<T> query) {
		return this.applyPagination(this.applyFAndS(query));
	}

	/**
	 * Overload of {@link #applyAll(SelectQuery)}.
	 *
	 * @param select
	 *            The {@link Select} whose underlying query to collate
	 * @return The underlying query, for method chaining purposes
	 */
	public <T extends Record> SelectQuery<T> applyAll(final SelectFinalStep<T> select) {
		return this.applyAll(select.getQuery());
	}

	private static class Filter {

		private final String field;
		private final String value;

		protected Filter(final String field, final String value) {
			this.field = field;
			this.value = value;
		}

		/**
		 * Computes a mapping {@link Function} that transforms {@link Filter}
		 * instances into <code>{@link Optional}<{@link Condition}></code>s,
		 * using a list of typed {@link Field}s references to generate
		 * type-specific {@link Condition}s.<br />
		 * <br />
		 * For example:
		 * <ul>
		 * <li>the {@link Condition} on a
		 * <code>{@link Field}<{@link String}></code> is set up to perform a
		 * <em>case-insensitive</em> sort, which wouldn't be possible with a
		 * {@link Condition} on a <code>{@link Field}<{@link Integer}></code>
		 * </li>
		 * <li>the {@link Condition} on a
		 * <code>{@link Field}<{@link Integer}></code> is set up to accept
		 * values like <code>/(lt|le|eq|ge|gt)(\d*\.)?\d+/</code> and actually
		 * perform a <em>mathematical comparison</em>, which wouldn't be
		 * possible with a {@link Condition} on a
		 * <code>{@link Field}<{@link Integer}></code></li>
		 * </ul>
		 * <br />
		 * <br />
		 * This method is meant to be used with{@link Stream#map(Function)}, in
		 * a stream chain as follows:
		 *
		 * <pre>
		 * stream
		 * 		.map({@link Filter#toCondition(List)})
		 * 		.filter(Optional::isPresent)
		 * 		.map(Optional::get);
		 * </pre>
		 *
		 * @param availableFields
		 *            The {@link Field}s that are available to the current
		 *            {@link SelectQuery}
		 * @return A {@link Function} to be used with
		 *         {@link Stream#map(Function)}
		 */
		public static Function<? super Filter, Optional<? extends Condition>> toCondition(final List<Field<?>> availableFields) {
			return self -> {
				final Optional<Field<?>> found = availableFields.stream().filter(f -> f.getName().equals(self.field)).findFirst();
				if (!found.isPresent()) {
					return Optional.empty();
				}

				if (Boolean.class.equals(found.get().getType())) {
					return Optional.of(DSL.field(self.field, Boolean.class).eq(DSL.field(self.value, Boolean.class)));

				}

				if (Number.class.isAssignableFrom(found.get().getType())) {
					return Optional.of(Filter.getNumberCondition(DSL.field(self.field, Double.class), self.value.substring(0, 2))
							.apply(DSL.field(self.value.substring(2), Double.class)));
				}

				return Optional.of(ResourcesHelper.unaccent(DSL.field(self.field, String.class))
						.containsIgnoreCase(ResourcesHelper.unaccent(DSL.val(self.value))));
			};
		}

		private static <T> Function<? super Field<T>, ? extends Condition> getNumberCondition(final Field<T> field, final String comparator) {
			switch (comparator) {
				case "lt":
					return field::lt;
				case "le":
					return field::le;
				case "eq":
					return field::eq;
				case "ge":
					return field::ge;
				case "gt":
					return field::gt;
				default:
					throw new IllegalArgumentException("Filter value for numeric fields must match /(lt|le|eq|ge|gt)(\\d*\\.)?\\d+/");
			}
		}
	}

	private static class Sort {

		private final String field;
		private final Function<? super Field<?>, ? extends SortField<?>> asSortField;

		protected Sort(final String field, final String order) {
			this.field = field;
			this.asSortField = "desc".equalsIgnoreCase(order) ? Field::desc : Field::asc;
		}

		/**
		 * Computes a mapping {@link Function} that transforms {@link Sort}
		 * instances into <code>{@link Optional}<{@link SortField}></code>s,
		 * using a list of typed {@link Field}s references to generate
		 * type-specific {@link SortField}s.<br />
		 * <br />
		 * For example, the <code>{@link SortField}<{@link String}></code> is
		 * set up to perform a <em>case-insensitive</em> sort, which wouldn't be
		 * possible with a <code>{@link SortField}<{@link Integer}></code>.
		 * <br />
		 * <br />
		 * This method is meant to be used with{@link Stream#map(Function)}, in
		 * a stream chain as follows:
		 *
		 * <pre>
		 * stream
		 * 		.map({@link Sort#toSortField(List)})
		 * 		.filter(Optional::isPresent)
		 * 		.map(Optional::get);
		 * </pre>
		 *
		 * @param availableFields
		 *            The {@link Field}s that are available to the current
		 *            {@link SelectQuery}
		 * @return A {@link Function} to be used with
		 *         {@link Stream#map(Function)}
		 */
		@SuppressWarnings("null")
		public static Function<? super Sort, ? extends Optional<SortField<?>>> toSortField(final List<Field<?>> availableFields) {
			return self -> {
				final Optional<Field<?>> found = availableFields.stream().filter(f -> f.getName().equals(self.field)).findFirst();
				if (!found.isPresent()) {
					return Optional.empty();
				}

				if (String.class.equals(found.get().getType())) {
					return Optional.of(self.asSortField.apply(ResourcesHelper.unaccent(DSL.field(self.field, String.class))));
				}

				return Optional.of(self.asSortField.apply(DSL.field(self.field)));
			};
		}
	}
}
