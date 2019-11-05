package org.ccjmne.orca.api.inject.business;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BinaryOperator;
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
import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.api.utils.Fields;
import org.ccjmne.orca.api.utils.JSONFields;
import org.eclipse.jdt.annotation.Nullable;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.Select;
import org.jooq.SelectFinalStep;
import org.jooq.SelectQuery;
import org.jooq.SortField;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.tools.Convert;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
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

  private static final Pattern SORT_ENTRY    = Pattern.compile("^sort\\[(?<field>[^]]+)\\]=(?<direction>.*)$", Pattern.CASE_INSENSITIVE);
  private static final Pattern FILTER_ENTRY  = Pattern.compile("^filter\\[(?<field>[^]]+)\\]=(?:(?<connect>and|or):)?(?<values>.*)$", Pattern.CASE_INSENSITIVE);
  private static final Pattern CONNECT_ENTRY = Pattern.compile("^connect\\[(?<field>[^]]+)\\]=(?<connector>and|or)$", Pattern.CASE_INSENSITIVE);

  private final List<? extends Sort>         orderBy;
  private final List<? extends FilterConfig> filterWhere;

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
        .map(m -> new Sort(new ParsedField(m.group("field")), m.group("direction")))
        .collect(Collectors.toList());
    this.filterWhere = uriInfo.getQueryParameters().entrySet().stream().flatMap(e -> e.getValue().stream().map(v -> String.format("%s=%s", e.getKey(), v)))
        .map(FILTER_ENTRY::matcher)
        .filter(Matcher::matches)
        .map(m -> new FilterConnected(new ParsedField(m.group("field")), m.group("connect"), m.group("values")))
        .collect(Collectors.toList());
    this.connectors = uriInfo.getQueryParameters().entrySet().stream().flatMap(e -> e.getValue().stream().map(v -> String.format("%s=%s", e.getKey(), v)))
        .map(CONNECT_ENTRY::matcher)
        .filter(Matcher::matches)
        .collect(Collectors.<Matcher, String, Function<? super Collection<Condition>, ? extends Condition>> toMap(m -> new ParsedField(m.group("field"))
            .getName(), m -> "and".equalsIgnoreCase(m.group("connector")) ? DSL::and : DSL::or, (first, second) -> second));
  }

  /**
   * Applicable at <strong>any depth of (sub-)query</strong>.<br />
   * <br />
   * Creates a <strong>new</strong> query: see
   * {@link RecordsCollator#applyFAndS(SelectQuery)}.
   *
   * @param query
   *          The {@link SelectQuery} to which sorting should be applied
   * @return A new, filtered and sorted {@code SelectQuery}
   */
  public <T extends Record> SelectQuery<T> applyFiltering(final SelectQuery<T> query) {
    return this.applyFilteringImpl(DSL.selectFrom(query.asTable()).getQuery());
  }

  /**
   * Overload of {@link #applyFiltering(SelectQuery)}.<br />
   * <br />
   * Creates a <strong>new</strong> query: see
   * {@link RecordsCollator#applyFAndS(SelectQuery)}.
   *
   * @param select
   *          The {@link Select} whose underlying query to collate
   * @return A new, filtered and sorted {@code SelectQuery}
   */
  public <T extends Record> SelectQuery<T> applyFiltering(final SelectFinalStep<T> select) {
    return this.applyFiltering(select.getQuery());
  }

  /**
   * Ideally <strong>only applied onto the outermost query</strong> that
   * actually gets returned to the client.<br />
   * Sorting the results of a sub-query used internally serves no purpose.<br />
   * <br />
   * Creates a <strong>new</strong> query: see
   * {@link RecordsCollator#applyFAndS(SelectQuery)}.
   *
   * @param query
   *          The {@link SelectQuery} to which sorting should be applied
   * @return A new, filtered and sorted {@code SelectQuery}
   */
  public <T extends Record> SelectQuery<T> applySorting(final SelectQuery<T> query) {
    return this.applySortingImpl(DSL.selectFrom(query.asTable()).getQuery());
  }

  /**
   * Overload of {@link #applySorting(SelectQuery)}.<br />
   * <br />
   * Creates a <strong>new</strong> query: see
   * {@link RecordsCollator#applyFAndS(SelectQuery)}.
   *
   * @param select
   *          The {@link Select} whose underlying query to collate
   * @return A new, filtered and sorted {@code SelectQuery}
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
   * {@link #applySorting(SelectQuery)}).
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
                              .map(FilterConfig.toCondition(Arrays.asList(query.fields())))
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

  private interface FilterConfig {

    public abstract @Nullable Optional<? extends FieldCondition<?>> getFieldCondition(final List<Field<?>> availableFields);

    /**
     * Computes a mapping {@link Function} that transforms {@link FilterConfig}
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
     * values like <code>/(ne|lt|le|eq|ge|gt)(\d*\.)?\d+/</code> and actually
     * perform a <em>mathematical comparison</em>, which wouldn't be
     * possible with a {@link Condition} on a
     * <code>{@link Field}<{@link String}></code></li>
     * </ul>
     * <br />
     * This method is meant to be used with {@link Stream#map(Function)}, in
     * a stream chain as follows:
     *
     * <pre>
     * stream
     *    .map({@link FilterConfig#toCondition(List)})
     *    .filter(Optional::isPresent)
     *    .map(Optional::get);
     * </pre>
     *
     * @param availableFields
     *          The {@link Field}s that are available to the current
     *          {@link SelectQuery}
     * @return A {@link Function} to be used with
     *         {@link Stream#map(Function)}
     */
    public static Function<FilterConfig, Optional<? extends FieldCondition<?>>> toCondition(final List<Field<?>> availableFields) {
      return ((Function<FilterConfig, Optional<? extends FieldCondition<?>>>) self -> self.getFieldCondition(availableFields));
    }
  }

  private static class FilterConnected implements FilterConfig {

    private static final Pattern PATTERN_VALUES = Pattern.compile("(?:(?<comparator>lt|le|eq|ge|gt|ne):)?(?<value>[^|]+)", Pattern.CASE_INSENSITIVE);

    private final List<FilterSingle>        filters = new ArrayList<>();
    private final BinaryOperator<Condition> connect;

    protected FilterConnected(final ParsedField field, final String connect, final String values) {
      this.connect = "and".equalsIgnoreCase(connect) ? DSL::and : DSL::or;
      final Matcher m = PATTERN_VALUES.matcher(values);
      while (m.find()) {
        this.filters.add(new FilterSingle(field, m.group("comparator"), m.group("value")));
      }
    }

    @Override
    public @Nullable Optional<? extends FieldCondition<?>> getFieldCondition(final List<Field<?>> availableFields) {
      return this.filters.stream()
          .map(FilterConfig.toCondition(availableFields))
          .filter(Optional::isPresent).map(Optional::get)
          .reduce(FieldCondition.connect(this.connect));
    }
  }

  private static class FilterSingle implements FilterConfig {

    private final ParsedField field;
    private final String      value;
    private final String      comparator;

    protected FilterSingle(final ParsedField field, final String comparator, final String value) {
      this.field = field;
      this.comparator = MoreObjects.firstNonNull(comparator, "");
      this.value = value;
    }

    @Override
    public @Nullable Optional<? extends FieldCondition<?>> getFieldCondition(final List<Field<?>> availableFields) {
      final Optional<Field<?>> preprocessed = this.field.preprocessFor(availableFields);
      if (!preprocessed.isPresent()) {
        return Optional.empty();
      }

      final Class<?> type = preprocessed.get().getType();
      return Optional.ofNullable(FieldCondition.on(preprocessed.get(), f -> {
        // Handle eq:null and ne:null
        if (Constants.FILTER_VALUE_NULL.equals(this.value)) {
          return "ne".equals(this.comparator) ? f.isNotNull() : f.isNull();
        }

        // If Boolean, interpret things like:
        // 1, 0, yes, no, Y, N, true, false, on, off, enabled and disabled
        if (Boolean.class.equals(type)) {
          return f.eq(Convert.convert(this.value, Boolean.class));
        }

        // If String, perform non-accented, case-insensitive partial match search
        if (String.class.equals(type)) {
          return f.containsIgnoreCase(Fields.unaccent(DSL.val(this.value, String.class)));
        }

        // If Number-like, Date or anything else, parse value as well as possible
        return FilterSingle.compare(f, this.comparator, DSL.val(this.value, type));
      }).as(this.field.getName()));
    }

    /**
     * Be <strong>sure</strong> both supplied {@code field} and {@code value}
     * actually <strong>are</strong> of compatible types!
     */
    @SuppressWarnings("unchecked")
    private static <T> Condition compare(final Field<?> field, final String comparator, final Field<T> value) {
      switch (comparator) {
        case "ne":
          return ((Field<T>) field).ne(value);
        case "lt":
          return ((Field<T>) field).lt(value);
        case "le":
          return ((Field<T>) field).le(value);
        case "ge":
          return ((Field<T>) field).ge(value);
        case "gt":
          return ((Field<T>) field).gt(value);
        case "eq":
        default:
          return ((Field<T>) field).eq(value);
      }
    }
  }

  private static class Sort {

    private final ParsedField field;

    private final Function<? super Field<?>, ? extends SortField<?>> asSortField;

    protected Sort(final ParsedField field, final String direction) {
      this.field = field;
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
        final Optional<Field<?>> preprocessed = self.field.preprocessFor(availableFields);
        return preprocessed.isPresent() ? Optional.of(self.asSortField.apply(preprocessed.get())) : Optional.empty();
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
     *
     * Be <strong>sure</strong> that both supplied {@code field} and
     * {@code getCondition} actually <strong>are</strong> of compatible types!
     */
    @SuppressWarnings("unchecked")
    protected static <T> FieldCondition<T> on(final Field<?> field, final Function<? super Field<T>, ? extends Condition> getCondition) {
      return new FieldCondition<>(field.getName(), getCondition.apply((Field<T>) field));
    }

    /**
     * Defines a normalised name for 'function' {@code Field}s.<br />
     * <br/>
     * Typically used for:
     * <ul>
     * <li>fields drilling into a {@code JSONB} with a specific {@code path}</li>
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

    /**
     * Computes a {@code BinaryOperator<FieldCondition>} that connects two
     * {@code FieldCondition}s using the specified {@code Condition} reducing
     * operation.
     *
     * @apiNote The {@code FieldCondition}s' names should be
     *          <strong>identical</strong>.
     *
     * @param connector
     *          The {@link BinaryOperator} connecting the underlying
     *          {@code Condition}s.
     * @return A new {@code BinaryOperator} able to connect {@code FieldCondition}s
     *         together
     */
    protected static BinaryOperator<FieldCondition<?>> connect(final BinaryOperator<Condition> connector) {
      return (a, b) -> {
        if (!a.name.equalsIgnoreCase(b.name)) {
          throw new IllegalArgumentException(String.format("Conditions on fields '%s' and '%s' shouldn't be connected", a.name, b.name));
        }

        return new FieldCondition<>(a.name, connector.apply(a.condition, b.condition));
      };
    }

    protected String getName() {
      return this.name;
    }

    protected Condition getCondition() {
      return this.condition;
    }
  }

  private static class ParsedField {

    private final static Pattern FIELD_PARSER = Pattern.compile("^(?:(?<preprocess>[^:]+):)?(?<name>[^.]+)(?:\\.(?<path>.+))?$");

    private static final Map<String, Function<? super Field<?>, ? extends Field<?>>> PREPROCESSORS = ImmutableMap
        .of("string", field -> Fields.unaccent(DSL.cast(field, String.class)),
            "date", field -> DSL.cast(field, Date.class),
            "number", field -> DSL.cast(field, Double.class));

    private final String           name;
    private final String           fullName;
    private final Optional<String> path;
    private final Optional<String> type;

    protected ParsedField(final String field) {
      final Matcher matcher = ParsedField.FIELD_PARSER.matcher(field);
      if (!matcher.matches()) {
        throw new IllegalArgumentException(String.format("Invalid field format: '%s'", field));
      }

      this.name = matcher.group("name");
      this.path = Optional.ofNullable(matcher.group("path"));
      this.type = Optional.ofNullable(matcher.group("preprocess"));
      this.fullName = this.path.isPresent() ? String.format("%s.%s", this.name, this.path.get()) : this.name;
    }

    protected String getName() {
      return this.name;
    }

    /**
     * May be of use eventually, to allow customising the connection between
     * multiple conditions on a single fully-qualified {@code jsonb} field leaf.
     */
    @SuppressWarnings("unused")
    protected String getFullName() {
      return this.fullName;
    }

    @SuppressWarnings("null")
    protected Optional<Field<?>> preprocessFor(final List<Field<?>> availableFields) {
      final Optional<Field<?>> found = availableFields.stream().filter(f -> f.getName().equals(this.name)).findFirst();
      if (!found.isPresent()) {
        return Optional.empty();
      }

      final Class<?> matchedType = found.get().getType();
      final String availableType = Number.class.isAssignableFrom(matchedType) ? "number" : matchedType.getSimpleName().toLowerCase();
      if (!this.path.isPresent()) {
        return Optional.of(PREPROCESSORS.getOrDefault(availableType, f -> f).apply(DSL.field(this.name, matchedType)));
      }

      if (!JSONB.class.equals(matchedType)) {
        throw new IllegalArgumentException(String.format("Couldn't access property '%s' on field '%s' of type '%s'", this.path.get(), this.name, matchedType));
      }

      final Field<Object> leaf = DSL.field("{0} #>> {1}", Object.class, DSL.field(this.name, JSONB.class), DSL.array(this.path.get().split("\\.")));
      if (!this.type.isPresent()) {
        return Optional.of(leaf);
      }

      if (!PREPROCESSORS.containsKey(this.type.get())) {
        throw new IllegalArgumentException(String.format("Invalid field type: '%s', should be one of: %s", this.type.get(), PREPROCESSORS.keySet()));
      }

      return Optional.of(PREPROCESSORS.get(this.type.get()).apply(leaf));
    }
  }
}
