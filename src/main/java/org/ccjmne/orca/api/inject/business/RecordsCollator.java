package org.ccjmne.orca.api.inject.business;

import java.sql.Date;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import org.apache.http.client.utils.URLEncodedUtils;
import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.api.utils.Fields;
import org.ccjmne.orca.api.utils.JSONFields;
import org.eclipse.jdt.annotation.NonNull;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Select;
import org.jooq.SelectFinalStep;
import org.jooq.SelectQuery;
import org.jooq.SortField;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.tools.Convert;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;

/**
 * Provides filtering, sorting and pagination methods to be applied to any
 * {@link SelectQuery}.<br />
 * Its configuration is automatically obtained from the query parameters of the
 * current {@link HttpServletRequest}.
 *
 * @author ccjmne
 */
public class RecordsCollator {

  private static final String PARAMETER_NAME_PAGE_SIZE   = "page-size";
  private static final String PARAMETER_NAME_PAGE_OFFSET = "page-offset";

  private static final Pattern SORT_ENTRY    = Pattern.compile("^sort\\[(?<field>[^].]+)(?<path>(?:\\.[^]]+)*)\\]=(?<direction>.*)$");
  private static final Pattern FILTER_ENTRY  = Pattern
      .compile("^filter\\[(?<field>[^].]+)(?<path>(?:\\.[^]]+)*)\\]=(?:(?<comparator>lt|le|eq|ge|gt|ne):)?(?<value>.*)$");
  private static final Pattern CONNECT_ENTRY = Pattern.compile("^connect\\[(?<field>[^]]+)\\]=(?<connector>AND|OR)$", Pattern.CASE_INSENSITIVE);

  private final List<? extends Sort>   orderBy;
  private final List<? extends Filter> filterWhere;

  private final Map<String, Function<? super Collection<Condition>, ? extends Condition>> connectors;

  private final int limit;
  private final int offset;

  /**
   * RAII constructor that parses the context's query parameters and creates
   * the corresponding jOOQ {@link Condition}s and {@link SortField}s.<br />
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
        .map(SORT_ENTRY::matcher)
        .filter(Matcher::matches)
        .map(m -> new Sort(m.group("field"), m.group("path"), m.group("direction")))
        .collect(Collectors.toList());
    this.filterWhere = uriInfo.getQueryParameters().entrySet().stream().flatMap(e -> e.getValue().stream().map(v -> String.format("%s=%s", e.getKey(), v)))
        .map(FILTER_ENTRY::matcher)
        .filter(Matcher::matches)
        .map(m -> new Filter(m.group("field"), m.group("path"), m.group("comparator"), m.group("value")))
        .collect(Collectors.toList());
    this.connectors = uriInfo.getQueryParameters().entrySet().stream().flatMap(e -> e.getValue().stream().map(v -> String.format("%s=%s", e.getKey(), v)))
        .map(CONNECT_ENTRY::matcher)
        .filter(Matcher::matches)
        .collect(Collectors.<Matcher, String, Function<? super Collection<Condition>, ? extends Condition>> toMap(m -> m
            .group("field"), m -> "AND".equalsIgnoreCase(m.group("connector")) ? DSL::and : DSL::or));
  }

  /**
   * Applicable at <strong>any depth of (sub-)query</strong>.<br />
   *
   * @param query
   *          The {@link SelectQuery} to which sorting should be applied
   * @return The original query, for method chaining purposes
   */
  public <T extends Record> SelectQuery<T> applyFiltering(final SelectQuery<T> query) {
    return this.applyFiltering(DSL.selectFrom(query.asTable()));
  }

  /**
   * Overload of {@link #applyFiltering(SelectQuery)}.
   *
   * @param select
   *          The {@link Select} whose underlying query to collate
   * @return The underlying query, for method chaining purposes
   */
  public <T extends Record> SelectQuery<T> applyFiltering(final SelectFinalStep<T> select) {
    return this.applyFilteringImpl(select.getQuery());
  }

  /**
   * Ideally <strong>only applied onto the outermost query</strong> that
   * actually gets returned to the client.<br />
   * Sorting the results of a sub-query used internally serves no purpose.
   *
   * @param query
   *          The {@link SelectQuery} to which sorting should be applied
   * @return The original query, for method chaining purposes
   */
  public <T extends Record> SelectQuery<T> applySorting(final SelectQuery<T> query) {
    return this.applySortingImpl(DSL.selectFrom(query.asTable()).getQuery());
  }

  /**
   * Overload of {@link #applySorting(SelectQuery)}.
   *
   * @param select
   *          The {@link Select} whose underlying query to collate
   * @return The underlying query, for method chaining purposes
   */
  public <T extends Record> SelectQuery<T> applySorting(final SelectFinalStep<T> select) {
    return this.applySorting(select.getQuery());
  }

  /**
   * Transforms the query into a <em>paginated response</em>, of the following
   * form:
   *
   * <pre>
   *        +------------+------------+-----------+-------------+---------------+
   *        | rslt_count | rslt_pages | page_size | page_offset | page_contents |
   * +------+------------+------------+-----------+-------------+---------------+
   * | Type | Integer    | Integer    | Integer   | Integer     | JSON Array    |
   * +------+------------+------------+-----------+-------------+---------------+
   * </pre>
   *
   * Should <strong>only ever be applied onto the outermost query</strong>
   * that actually gets returned to the client.<br />
   * Should <strong>never be used on a sub-query </strong>used internally to
   * refine the results.
   *
   * @param query
   *          The {@link SelectQuery} to which pagination should be applied
   * @return The original query, for method chaining purposes
   */
  public <T extends Record> SelectQuery<?> applyPagination(final SelectQuery<T> query) {
    final Table<T> data = query.asTable("resultset");
    if (this.limit > 0) {
      return DSL.with("resultset").as(query)
          .select(
                  DSL.select(DSL.count()).from("resultset").asField("rslt_count"),
                  DSL.select(DSL.ceil(DSL.count().div(Float.valueOf(this.limit))).as("rslt_pages")).from("resultset").asField("rslt_pages"),
                  DSL.val(Integer.valueOf(this.limit)).as("page_size"),
                  DSL.val(Integer.valueOf(this.offset / this.limit)).as("page_offset"),
                  DSL.select(JSONFields.arrayAgg(Fields.unqualify(data.fields()))).from(DSL.select().from("resultset").limit(this.offset, this.limit))
                      .asField("page_contents"))
          .getQuery();
    }

    return DSL.with("resultset").as(query)
        .select(
                DSL.select(DSL.count()).from("resultset").asField("rslt_count"),
                DSL.val(Integer.valueOf(1)).as("rslt_pages"),
                DSL.val(Integer.valueOf(0)).as("page_size"),
                DSL.val(Integer.valueOf(0)).as("page_offset"),
                DSL.select(JSONFields.arrayAgg(data.fields())).from("resultset").asField("page_contents"))
        .getQuery();
  }

  /**
   * Overload of {@link #applyPagination(SelectQuery)}.
   *
   * @param select
   *          The {@link Select} whose underlying query to which pagination should
   *          be applied
   * @return The underlying query, for method chaining purposes
   */
  public <T extends Record> SelectQuery<?> applyPagination(final SelectFinalStep<T> select) {
    return this.applyPagination(select.getQuery());
  }

  /**
   * Stands for <em>"apply filtering and sorting"</em>.<br />
   * <br />
   * Creates a <strong>new</strong> query like:
   *
   * <pre>
   * SELECT * FROM (
   *     [{@code query}]
   * ) AS aliased
   * WHERE aliased.[filters...]
   * ORDER BY aliased.[sort...]
   * </pre>
   *
   * Doing so is necessary, in order to be able to filter and sort on dynamic
   * columns (typically {@code jsonb} aggregations). Referencing the
   * {@code SELECT}ed fields from the original {@code query} argument should
   * be done through either {@link SelectQuery#fields(Field...)} or
   * {@link Constants#unqualify(Field...)}.<br />
   * <br />
   *
   * Ideally <strong>only applied onto the outermost query</strong> that
   * actually gets returned to the client.<br />
   * Sorting the results of a sub-query used internally serves no purpose (see
   * {@link #applySorting(SelectQuery)}.
   *
   * @param query
   *          The {@link SelectQuery} to which filtering and sorting should
   *          be applied
   * @return A new, filtered and sorted {@code SelectQuery}
   */
  public <T extends Record> SelectQuery<T> applyFAndS(final SelectQuery<T> query) {
    return this.applySortingImpl(this.applyFilteringImpl(DSL.selectFrom(query.asTable()).getQuery()));
  }

  /**
   * Overload of {@link #applyFAndS(SelectQuery)}.
   *
   * @param select
   *          The {@link Select} whose underlying query to collate
   * @return A new, filtered and sorted {@code SelectQuery}
   */
  public <T extends Record> SelectQuery<T> applyFAndS(final SelectFinalStep<T> select) {
    return this.applyFAndS(select.getQuery());
  }

  /**
   * Transforms the query into a <em>filtered</em>, <em>sorted</em> and
   * <em>paginated response</em>, of the following form:
   *
   * <pre>
   *        +------------+------------+-----------+-------------+---------------+
   *        | rslt_count | rslt_pages | page_size | page_offset | page_contents |
   * +------+------------+------------+-----------+-------------+---------------+
   * | Type | Integer    | Integer    | Integer   | Integer     | JSON Array    |
   * +------+------------+------------+-----------+-------------+---------------+
   * </pre>
   *
   * Should <strong>only ever be applied onto the outermost query</strong>
   * that actually gets returned to the client.<br />
   * Should <strong>never be used on a sub-query </strong>used internally to
   * refine the results (see {@link #applyPagination(SelectQuery)}). <br />
   * <br />
   *
   * @param query
   *          The {@link SelectQuery} to which filtering, sorting and
   *          pagination should be applied
   * @return A new, filtered, sorted and paginated {@code SelectQuery}
   */
  @Deprecated
  public SelectQuery<? extends Record> applyAll(final SelectQuery<? extends Record> query) {
    return this.applyPagination(this.applyFAndS(query));
  }

  /**
   * Overload of {@link #applyAll(SelectQuery)}.
   *
   * @param select
   *          The {@link Select} whose underlying query to collate
   * @return A new, filtered, sorted and paginated {@code SelectQuery}
   */
  @Deprecated
  public SelectQuery<? extends Record> applyAll(final SelectFinalStep<? extends Record> select) {
    return this.applyAll(select.getQuery());
  }

  /**
   * For internal use. Attempts to {@code FILTER} directly on the
   * <strong>supplied</strong> query.<br />
   * <br />
   * Applies all filter {@link Condition}s for each {@link Field} connected with
   * either:
   * <ul>
   * <li>{@link DSL#or(Condition...)} iff there is a query parameter such as
   * {@code connect[<field>]=OR}, or:</li>
   * <li>{@link DSL#and(Condition...)} otherwise</li>
   * </ul>
   */
  private <T extends Record> SelectQuery<T> applyFilteringImpl(final SelectQuery<T> query) {
    query.addConditions(Maps
        .transformEntries(
                          this.filterWhere.stream()
                              .map(Filter.toCondition(Arrays.asList(query.fields())))
                              .filter(Optional::isPresent).map(Optional::get)
                              .collect(Collectors.groupingBy(FieldCondition::getName)),
                          (f, conditions) -> this.connectors.getOrDefault(f, DSL::and).apply(Collections2.transform(conditions, FieldCondition::getCondition)))
        .values());
    return query;
  }

  /**
   * For internal use. Attempts to {@code ORDER BY} directly on the
   * <strong>supplied</strong> query.<br />
   * <br />
   * Uses the order in which the sort parameters appear in the request URI as
   * sorting priority.
   */
  private <T extends Record> SelectQuery<T> applySortingImpl(final SelectQuery<T> query) {
    query.addOrderBy(this.orderBy.stream()
        .map(Sort.toSortField(Arrays.asList(query.fields())))
        .filter(Optional::isPresent).map(Optional::get)
        .collect(Collectors.toList()));
    return query;
  }

  private static class Filter {

    private final String   field;
    private final String   value;
    private final String   comparator;
    private final String[] path;

    protected Filter(final String field, final String path, final String comparator, final String value) {
      this.field = field;
      this.path = path.isEmpty() ? new String[0] : path.substring(1).split("\\.");
      this.comparator = MoreObjects.firstNonNull(comparator, "");
      this.value = value;
    }

    /**
     * Computes a mapping {@link Function} that transforms {@link Filter}
     * instances into <code>{@link Optional}<{@link FieldCondition}></code>s,
     * using a list of typed {@link Field}s references to generate
     * type-specific {@link Condition}s.<br />
     * <br />
     * For example:
     * <ul>
     * <li>the {@link Condition} on a
     * <code>{@link Field}<{@link String}></code> is set up to perform a
     * <em>case-insensitive</em> filtering, which wouldn't be possible with a
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
     *          The {@link Field}s that are available to the current
     *          {@link SelectQuery}
     * @return A {@link Function} to be used with
     *         {@link Stream#map(Function)}
     */
    @SuppressWarnings("null")
    public static Function<? super Filter, Optional<? extends FieldCondition<?>>> toCondition(final List<Field<?>> availableFields) {
      return ((Function<? super Filter, FieldCondition<?>>) self -> {
        final Optional<Field<?>> found = availableFields.stream().filter(f -> f.getName().equals(self.field)).findFirst();
        if (!found.isPresent()) {
          return null;
        }

        // Handle eq:null and ne:null for shallow Fields
        if ((self.path.length == 0) && Constants.FILTER_VALUE_NULL.equals(self.value)) {
          return FieldCondition.on(DSL.field(self.field), f -> self.comparator.equals("eq") ? f.isNull() : f.isNotNull());
        }

        // If String, perform non-accented, case-insensitive partial match search
        if (String.class.equals(found.get().getType())) {
          return FieldCondition.on(Fields.unaccent(DSL.field(self.field, String.class)), f -> f.containsIgnoreCase(Fields.unaccent(DSL.val(self.value))))
              .as(self.field);
        }

        // If Number-like, parse it first
        if (Number.class.isAssignableFrom(found.get().getType())) {
          return FieldCondition.on(DSL.field(self.field, Double.class), f -> Filter.compare(f, self.comparator, DSL.val(self.value, Double.class)));
        }

        // It Date, parse first
        if (Date.class.equals(found.get().getType())) {
          return FieldCondition.on(DSL.field(self.field, Date.class), f -> Filter.compare(f, self.comparator, DSL.val(self.value, Date.class)));
        }

        // If Boolean, interpret things like:
        // 1, 0, yes, no, Y, N, true, false, on, off, enabled, and: disabled
        if (Boolean.class.equals(found.get().getType())) {
          return FieldCondition.on(DSL.field(self.field, Boolean.class), f -> f.eq(Convert.convert(self.value, Boolean.class)));
        }

        if (JsonNode.class.equals(found.get().getType())) {
          return ((Supplier<FieldCondition<?>>) () -> {
            final Field<String> leaf = DSL.field("{0} #>> {1}", String.class, DSL.field(self.field, JsonNode.class), DSL.array(self.path));

            // Handle eq:null and ne:null
            if (Constants.FILTER_VALUE_NULL.equals(self.value)) {
              return FieldCondition.on(leaf, f -> self.comparator.equals("eq") ? f.isNull() : f.isNotNull());
            }

            if (null != DSL.<Float> val(self.value, Float.class).getValue()) { // Can be interpreted as Numeric
              return FieldCondition.on(DSL.cast(leaf, Float.class), f -> Filter.compare(f, self.comparator, DSL.val(self.value, Float.class)));
            }

            return FieldCondition.on(leaf, f -> Filter.compare(f, self.comparator, DSL.val(self.value)));
          }).get().as(String.format("%s.%s", self.field, Joiner.on(".").join(self.path)));
        }

        // Default: regular comparison
        return FieldCondition.on(DSL.field(self.field), f -> Filter.compare(f, self.comparator, DSL.field(self.value)));
      }).andThen(Optional::ofNullable);
    }

    private static <T> Condition compare(final Field<T> field, final String comparator, final Field<T> value) {
      switch (comparator) {
        case "ne":
          return field.ne(value);
        case "lt":
          return field.lt(value);
        case "le":
          return field.le(value);
        case "ge":
          return field.ge(value);
        case "gt":
          return field.gt(value);
        case "eq":
        default:
          return field.eq(value);
      }
    }
  }

  private static class Sort {

    private final String   field;
    private final String[] path;

    private final Function<? super Field<?>, ? extends SortField<?>> asSortField;

    protected Sort(final String field, final String path, final String direction) {
      this.field = field;
      this.path = path.isEmpty() ? new String[0] : path.substring(1).split("\\.");
      this.asSortField = f -> (Constants.SORT_DIRECTION_DESC.equalsIgnoreCase(direction) ? f.desc() : f.asc()).nullsLast();
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
     *          The {@link Field}s that are available to the current
     *          {@link SelectQuery}
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
          return Optional.of(self.asSortField.apply(Fields.unaccent(DSL.field(self.field, String.class))));
        }

        if (JsonNode.class.equals(found.get().getType())) {
          return Optional.of(self.asSortField.apply(DSL.field("{0} #> {1}", JsonNode.class, DSL.field(self.field, JsonNode.class), DSL.array(self.path))));
        }

        return Optional.of(self.asSortField.apply(DSL.field(self.field)));
      };
    }
  }

  private static class FieldCondition<T> {

    private final String    name;
    private final Condition condition;

    /**
     * Instantiates a new {@code FieldCondition}. For example:
     *
     * <pre>
     * final {@code Field<Integer>} field = DSL.field("my_int", Integer.class);
     * final {@code FieldCondition<Integer>} fieldCondition = FieldCondition
     *     .on(field, f -> f.isNotNull());
     * </pre>
     */
    protected static <T> FieldCondition<T> on(@NonNull final Field<T> field, final Function<? super Field<T>, ? extends Condition> getCondition) {
      return new FieldCondition<>(field.getName(), getCondition.apply(field));
    }

    /**
     * Defines a normalised name for 'function' {@code Field}s.<br />
     * <br/>
     * Typically used for:
     * <ul>
     * <li>fields drilling into a {@code JsonNode} with a specific {@code path}</li>
     * <li>fields that are encapsulated in a function, like
     * {@code unaccent(<field>)}</li>
     * </ul>
     *
     * @param normalised
     *          The new name to be used
     * @return A copy of {@code this}, with the specified {@code normalised} name
     */
    protected FieldCondition<T> as(final String normalised) {
      return new FieldCondition<>(normalised, this.condition);
    }

    private FieldCondition(final String name, final Condition condition) {
      this.name = name;
      this.condition = condition;
    }

    protected String getName() {
      return this.name;
    }

    protected Condition getCondition() {
      return this.condition;
    }
  }
}
