package org.ccjmne.orca.api.modules;

import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

	private final List<? extends FieldOrdering> orderBy;
	private final List<? extends FieldCondition> filterWhere;
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
		this.limit = pSize != null ? Integer.valueOf(pSize).intValue() : 0;
		this.offset = this.limit * Integer.valueOf(MoreObjects.firstNonNull(uriInfo.getQueryParameters().getFirst(PARAMETER_NAME_PAGE_OFFSET), "0")).intValue();
		this.orderBy = URLEncodedUtils.parse(uriInfo.getRequestUri(), "UTF-8").stream().map(p -> String.format("%s=%s", p.getName(), p.getValue()))
				.map(SORTING_ENTRY::matcher)
				.filter(Matcher::matches)
				.map(m -> new FieldOrdering(m.group("field"), m.group("order")))
				.collect(Collectors.toList());
		this.filterWhere = uriInfo.getQueryParameters().entrySet().stream().map(e -> String.format("%s=%s", e.getKey(), e.getValue().get(0)))
				.map(FILTER_ENTRY::matcher)
				.filter(Matcher::matches)
				.map(m -> new FieldCondition(m.group("field"), m.group("value")))
				.collect(Collectors.toList());
	}

	public <T extends Record> SelectQuery<T> applyFiltering(final SelectQuery<T> query) {
		query.addConditions(this.filterWhere.stream()
				.filter(FieldCondition.belongsTo(RecordsCollator.getAvailableFields(query))).map(FieldCondition::getCondition)
				.collect(Collectors.toList()));
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
				.filter(FieldOrdering.belongsTo(RecordsCollator.getAvailableFields(query))).map(FieldOrdering::getSortField)
				.collect(Collectors.toList()));
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

	private static <T extends Record> List<String> getAvailableFields(final SelectQuery<T> query) {
		return query.fieldStream().map(Field::getName).collect(Collectors.toList());
	}

	private static class FieldCondition {

		private final Condition condition;
		private final String field;

		// TODO: Handle non-varchar data w/ something other than ILIKE
		protected FieldCondition(final String field, final String value) {
			this.condition = ResourcesHelper.unaccent(DSL.field(field, String.class)).containsIgnoreCase(ResourcesHelper.unaccent(DSL.val(value)));
			this.field = field;
		}

		public Condition getCondition() {
			return this.condition;
		}

		public static Predicate<? super FieldCondition> belongsTo(final List<String> availableFields) {
			return f -> availableFields.contains(f.field);
		}
	}

	private static class FieldOrdering {

		private final SortField<?> order;
		private final String field;

		// TODO: Handle non-varchar data w/ something other than ILIKE
		protected FieldOrdering(final String field, final String order) {
			final Field<String> unaccented = ResourcesHelper.unaccent(DSL.field(field, String.class));
			this.order = "desc".equalsIgnoreCase(order) ? unaccented.desc() : unaccented.asc();
			this.field = field;
		}

		public SortField<?> getSortField() {
			return this.order;
		}

		public static Predicate<? super FieldOrdering> belongsTo(final List<String> availableFields) {
			return f -> availableFields.contains(f.field);
		}
	}
}
