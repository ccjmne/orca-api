package org.ccjmne.orca.api.modules;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import org.apache.http.client.utils.URLEncodedUtils;
import org.jooq.Record;
import org.jooq.SelectQuery;
import org.jooq.impl.DSL;

/**
 * Provides filtering, sorting and pagination methods to be applied to any
 * {@link SelectQuery}.<br />
 * Its configuration is automatically obtained from the query parameters of the
 * current {@link HttpServletRequest}.
 *
 * @author ccjmne
 */
public class RecordsCollator {

	private static final String PARAMETER_NAME_LIMIT = "page-size";
	private static final String PARAMETER_NAME_OFFSET = "page-offset";
	private static final Pattern SORTING_ENTRY = Pattern.compile("^sorting\\[(?<key>[^]]+)\\]=(?<value>.*)$");
	private static final Pattern FILTER_ENTRY = Pattern.compile("^filter\\[(?<key>[^]]+)\\]=(?<value>.*)$");

	private final UriInfo uriInfo;
	private final Predicate<Matcher> fieldsFilter;

	@Inject
	public RecordsCollator(@Context final UriInfo uriInfo) {
		this.uriInfo = uriInfo;
		this.fieldsFilter = x -> true;
	}

	private RecordsCollator(final UriInfo uriInfo, final String fieldsPrefix) {
		this.uriInfo = uriInfo;
		this.fieldsFilter = x -> x.group("key").startsWith(fieldsPrefix);
	}

	/**
	 * Creates a new {@link RecordsCollator} that can only even attempt to
	 * filter and sort onto fields whose names start with the specified prefix.
	 *
	 * @param fieldsPrefix
	 *            The prefix discriminating the fields this
	 *            {@link RecordsCollator} can work with
	 * @return A copy of <code>this</code> {@link RecordsCollator} that
	 *         disregards fields that do not match the prefix provided
	 */
	public RecordsCollator restrictTo(final String fieldsPrefix) {
		return new RecordsCollator(this.uriInfo, fieldsPrefix);
	}

	// TODO: Handle non-varchar data w/ something other than ILIKE
	public <T extends Record> SelectQuery<T> applyFiltering(final SelectQuery<T> query) {
		query.addConditions(this.transform(FILTER_ENTRY::matcher, m -> DSL.field(m.group("key")).containsIgnoreCase(m.group("value"))));
		return query;
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
		query.addOrderBy(this.transformInOrder(	SORTING_ENTRY::matcher,
												m -> m.group("value").equalsIgnoreCase("desc")	? DSL.field(m.group("key")).desc()
																								: DSL.field(m.group("key")).asc()));
		return query;
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
		query.addLimit(	DSL.val(this.uriInfo.getQueryParameters().getFirst(PARAMETER_NAME_OFFSET), Integer.class),
						DSL.val(this.uriInfo.getQueryParameters().getFirst(PARAMETER_NAME_LIMIT), Integer.class));
		return query;
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
		this.applyFiltering(query);
		this.applySorting(query);
		return query;
	}

	/**
	 * Should <strong>only ever be applied onto the outermost query</strong>
	 * that actually gets returned to the client.<br />
	 * Should <strong>never be used on a sub-query </strong>used internally to
	 * refine the results (see {@link #applyPagination(SelectQuery)}).
	 *
	 * @param query
	 *            The {@link SelectQuery} to which pagination should be applied
	 * @return The original query, for method chaining purposes
	 */
	public <T extends Record> SelectQuery<T> applyAll(final SelectQuery<T> query) {
		this.applyFiltering(query);
		this.applySorting(query);
		this.applyPagination(query);
		return query;
	}

	private <T> List<T> transform(final Function<? super String, ? extends Matcher> matcher, final Function<Matcher, ? extends T> mapper) {
		return this.uriInfo.getQueryParameters().entrySet().stream()
				.map(e -> String.format("%s=%s", e.getKey(), e.getValue().get(0)))
				.map(matcher)
				.filter(Matcher::matches)
				.filter(this.fieldsFilter)
				.map(mapper)
				.collect(Collectors.toList());
	}

	/**
	 * Preserves the order in which the query parameters appear. Used for the
	 * sorting options, where their sequence order is crucial.
	 *
	 * @param matcher
	 * @param mapper
	 * @return
	 */
	private <T> List<T> transformInOrder(final Function<? super String, ? extends Matcher> matcher, final Function<Matcher, ? extends T> mapper) {
		return URLEncodedUtils.parse(this.uriInfo.getRequestUri(), "UTF-8").stream()
				.map(p -> String.format("%s=%s", p.getName(), p.getValue()))
				.map(matcher)
				.filter(Matcher::matches)
				.filter(this.fieldsFilter)
				.map(mapper)
				.collect(Collectors.toList());
	}
}
