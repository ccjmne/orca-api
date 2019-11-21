package org.ccjmne.orca.api.inject.business;

import java.sql.Date;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import org.ccjmne.orca.api.rest.utils.QuickSearchEndpoint;
import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.api.utils.Fields;
import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.Nullable;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Param;
import org.jooq.impl.DSL;

import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;

/**
 * Provides tools to extract arguments from the current
 * {@link HttpServletRequest}.
 */
public class QueryParams {

  private static final Param<JSONB> TAGS_FIELD_UNIVERSAL = DSL.val(JSONB.valueOf(new TextNode(Constants.TAGS_VALUE_UNIVERSAL).toString()));

  // Resources identifiers
  public static final FieldType<Integer> SITE        = new FieldType<>("site", Integer.class);
  public static final FieldType<Integer> EMPLOYEE    = new FieldType<>("employee", Integer.class);
  public static final FieldType<Integer> TRAINER     = new FieldType<>("trainer", Integer.class);
  public static final FieldType<Integer> SESSION     = new FieldType<>("session", Integer.class);
  public static final FieldType<Integer> CERTIFICATE = new FieldType<>("certificate", Integer.class);

  // Virtual resources identifiers
  public static final FirstParamType<Field<JSONB>>           GROUP_BY    = new FirstParamType<>("group-by", v -> Constants.TAGS_VALUE_UNIVERSAL
      .equals(v) ? TAGS_FIELD_UNIVERSAL : DSL.field("site_tags -> {0}", JSONB.class, v), TAGS_FIELD_UNIVERSAL);
  public static final DependentType<Field<JSONB>, Condition> GROUP_VALUE = new DependentType<>("group-value", QueryParams.GROUP_BY,
                                                                                               QueryParams::computeGroupValue, v -> DSL.noCondition());

  // Collation flags
  public static final FieldType<Boolean> INCLUDE_DECOMISSIONED = new FieldType<>("include-decommissioned", Boolean.class);
  public static final FieldType<Boolean> FILTER_BY_SESSIONS    = new FieldType<>("filter-by-sessions", Boolean.class);

  // Temporal selectors
  public static final FirstParamType<Field<Date>>             DATE          = new FirstParamType<>("date", v -> DSL.val(v, Date.class), DSL.currentDate());
  public static final DependentType<Field<Date>, Field<Date>> FROM          = new DependentType<>("from", QueryParams.DATE, QueryParams::parseDate,
                                                                                                  d -> Fields.DATE_NEGATIVE_INFINITY);
  public static final DependentType<Field<Date>, Field<Date>> TO            = new DependentType<>("to", QueryParams.DATE, QueryParams::parseDate,
                                                                                                  d -> Fields.DATE_INFINITY);
  public static final DependentType<Field<Date>, Field<Date>> FROM_OR_TODAY = new DependentType<>("from", QueryParams.DATE, QueryParams::parseDate,
                                                                                                  d -> d);
  public static final DependentType<Field<Date>, Field<Date>> TO_OR_TODAY   = new DependentType<>("to", QueryParams.DATE, QueryParams::parseDate,
                                                                                                  d -> d);
  public static final FirstParamType<Field<Date>>             INTERVAL      = new FirstParamType<>("interval", v -> DSL
      .field("{0}::interval", Date.class, v), DSL.field("'1 month'::interval", Date.class));

  // Quick-search parameters
  public static final FieldType<String>                       SEARCH_TERMS  = new FieldType<>("q", String.class);
  public static final AllParamsType<List<String>>             RESOURCE_TYPE = new AllParamsType<>("type", v -> v, QuickSearchEndpoint.RESOURCES_TYPES);
  public static final DependentType<Field<Date>, Field<Date>> SESSION_DATE  = new DependentType<>("session-date", QueryParams.DATE, QueryParams::parseDate,
                                                                                                  d -> d);

  private static final Pattern IS_INFINITY_DATE = Pattern.compile("^-?infinity$");
  private static final Pattern IS_RELATIVE_DATE = Pattern.compile("^[+-]");

  private final Map<Type<?, ?>, Object> types;

  /**
   * Parses the request URL for arguments.<br />
   * <br />
   * When an argument is defined in both the {@code pathParameters} and the
   * {@code queryParameters}, the <strong>path</strong> one takes precedence
   * while the other is discarded.
   *
   * @param uriInfo
   *          Injected. The {@link UriInfo} extracted from the corresponding
   *          {@link HttpServletRequest}
   */
  @Inject
  public QueryParams(@Context final UriInfo uriInfo) {
    this.types = Stream.concat(uriInfo.getQueryParameters().entrySet().stream(), uriInfo.getPathParameters().entrySet().stream())
        .flatMap(Type::mapper).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (fromQueryParams, fromPath) -> fromPath));
  }

  public <T> Optional<T> of(final Type<?, T> type) {
    return Optional.ofNullable(this.get(type));
  }

  /**
   * Always {@code true} for {@code Type}s that have a default value.
   *
   * @see QueryParams#isDefault(Type)
   */
  public <T> boolean has(final Type<?, T> type) {
    return this.of(type).isPresent();
  }

  /**
   * Overload of {@link QueryParams#has(Type)} for {@link FieldType} to account
   * for parameters that couldn't be coerced automatically.
   */
  public <T> boolean has(final FieldType<T> type) {
    return this.of(type).isPresent() && (this.getRaw(type) != null);
  }

  /**
   * Is {@code true} when the given {@code type} <strong>does have a default
   * value<strong> that wasn't overridden by the query parameters.
   */
  public <T> boolean isDefault(final Type<?, T> type) {
    // Don't use QueryParams#has or QueryParams#of, which treat unspecified
    // parameters with default values as being actually present
    return (type.orElse != null) && !this.types.containsKey(type);
  }

  public <T> boolean is(final FieldType<T> type, @NonNull final T value) {
    return this.has(type) && value.equals(this.getRaw(type));
  }

  /**
   * Convenience method for {@link QueryParams#is(type, Boolean.TRUE)}.
   */
  // TODO: use this instead of QueryParams#is(..., Boolean.TRUE)
  public boolean isEnabled(final FieldType<Boolean> type) {
    return this.has(type) && Boolean.TRUE.equals(this.getRaw(type));
  }

  @SuppressWarnings("unchecked") // always safe
  public <T> T get(final Type<?, T> type) {
    return (T) this.types.getOrDefault(type, type.orElse);
  }

  /**
   * Overload of {@link QueryParams#get(Type)} for {@link DependentType}.
   */
  @SuppressWarnings("unchecked") // always safe
  public <U, T> T get(final DependentType<U, T> type) {
    return ((Function<? super U, ? extends T>) this.types.getOrDefault(type, type.orElse)).apply(this.get(type.getDependency()));
  }

  public <T> T getRaw(final FieldType<T> type) {
    return this.get(type).getValue();
  }

  /**
   * Returns the given {@code Type}'s parameter iff it was provided by the
   * request.<br />
   * Otherwise, returns {@code orElse}, <strong>regardless of whether
   * the given {@code Type} specifies a default value<strong>.
   */
  public <T> T getOrDefault(final Type<?, T> type, final @NonNull T orElse) {
    return this.types.containsKey(type) ? this.get(type) : orElse;
  }

  /**
   * Overload of {@link QueryParams#getOrDefault(Type, Object)} for
   * {@link DependentType}.
   */
  public <U, T> T getOrDefault(final DependentType<U, T> type, final @NonNull T orElse) {
    return this.types.containsKey(type) ? this.get(type) : orElse;
  }

  /**
   * Like a {@link FieldType}, except that:
   * <ul>
   * <li>it can coerce parameters to any arbitrary {@code T} class</li>
   * <li>it can have a default value</li>
   * </ul>
   *
   * @param <U>
   *          For internal use only.
   * @param <T>
   *          The class of this {@code Type}'s coerced values
   * @see FirstParamType
   * @see AllParamsType
   */
  private static abstract class Type<U, T> {

    private static final Map<String, List<Type<?, ?>>> TYPES = new HashMap<>();

    protected final @Nullable T orElse;

    private final Function<? super U, ? extends T> coercer;

    protected Type(final String name, final Function<? super U, ? extends T> coercer, @Nullable final T orElse) {
      this.coercer = coercer;
      this.orElse = orElse;
      TYPES.compute(name, (k, v) -> v == null
                                              ? Collections.singletonList(this)
                                              : ImmutableList.<Type<?, ?>> builder().addAll(v).add(this).build());
    }

    @SuppressWarnings("unchecked") // always safe
    protected static final Stream<? extends Map.Entry<Type<?, ?>, ?>> mapper(final Map.Entry<String, List<String>> source) {
      final List<Type<?, ?>> types = TYPES.getOrDefault(source.getKey(), Collections.EMPTY_LIST);
      return (Stream<? extends Entry<Type<?, ?>, ?>>) types.stream().map(t -> t.map(source.getValue()));
    }

    private final Map.Entry<Type<U, T>, T> map(final List<String> parameters) {
      return new SimpleEntry<>(this, this.coercer.apply(this.supplyCoercer(parameters)));
    }

    protected abstract U supplyCoercer(final List<String> parameters);
  }

  /**
   * A {@link Type} that consumes all the matching query parameters as a
   * {@code List<String>}.
   *
   * @param <T>
   *          The class of this {@code Type}'s coerced values
   */
  private static class AllParamsType<T> extends Type<List<String>, T> {

    protected AllParamsType(final String name, final Function<? super List<String>, ? extends T> coercer, final @Nullable T orElse) {
      super(name, coercer, orElse);
    }

    @Override
    protected List<String> supplyCoercer(final List<String> parameters) {
      return parameters;
    }
  }

  /**
   * A {@link Type} that only considers the first matching query parameter.
   *
   * @param <T>
   *          The class of this {@code Type}'s coerced values
   */
  private static class FirstParamType<T> extends Type<String, T> {

    protected FirstParamType(final String name, final Function<String, ? extends T> coercer, final @Nullable T orElse) {
      super(name, coercer, orElse);
    }

    @Override
    protected String supplyCoercer(final List<String> parameters) {
      return parameters.get(0);
    }
  }

  /**
   * A {@link FirstParamType} that tries to coerce parameters to a jOOQ
   * {@link Field} of a certain type and provides no default value.
   *
   * @param <T>
   *          The type of the {@code Field} to coerce parameters as
   */
  private static class FieldType<T> extends FirstParamType<Param<T>> {

    protected FieldType(final String name, final Class<T> type) {
      super(name, v -> DSL.val(v, type), null);
    }
  }

  /**
   * A {@link FirstParamType} that may depend on the value of another {@link Type}
   * to be virtually injected by the overloaded
   * {@link QueryParams#get(DependentType)}.
   *
   * @param <U>
   *          This {@code Type} dependency's coercion class
   * @param <T>
   *          The class of this {@code Type}'s coerced values
   */
  private static class DependentType<U, T> extends FirstParamType<Function<? super U, ? extends T>> {

    private final Type<?, ? extends U> dependency;

    protected DependentType(
                            final String name,
                            final Type<?, ? extends U> dependency,
                            final Function<String, Function<? super U, ? extends T>> coercer,
                            final @Nullable Function<? super U, ? extends T> orElse) {
      super(name, coercer, orElse);
      this.dependency = dependency;
    }

    protected Type<?, ? extends U> getDependency() {
      return this.dependency;
    }
  }

  /**
   * Used to parse either:
   * <ul>
   * <li>an absolute date (e.g.: {@code "2019-01-01"}), or</li>
   * <li>an interval relative to a reference (e.g.: {@code "+1 month"}), or</li>
   * <li>{@code /^-?infinity$/}, to represent the unbounded end of a range</li>
   * </ul>
   */
  private static Function<? super Field<Date>, ? extends Field<Date>> parseDate(final String dateStr) {
    if (IS_INFINITY_DATE.matcher(dateStr).matches()) {
      // Can't parse "infinity"-type dates w/ DSL#val
      return unused -> DSL.field("{0}::date", Date.class, dateStr);
    }

    if (IS_RELATIVE_DATE.matcher(dateStr).find(0)) {
      return reference -> reference.plus(DSL.field("{0}::interval", Date.class, dateStr));
    }

    return unused -> DSL.val(dateStr, Date.class);
  }

  /**
   * Builds up on the {@link QueryParams#GROUP_BY} parameter to select a specific
   * value taken by that tag to uniquely identify a virtual sites-group.
   */
  private static Function<? super Field<JSONB>, ? extends Condition> computeGroupValue(final String value) {
    if (Constants.TAGS_VALUE_NONE.equals(value)) {
      return tag -> tag.isNull();
    }

    return tag -> tag.eq(JSONB.valueOf("true".equals(value) || "false".equals(value) ? value : new TextNode(value).toString()));
  }
}
