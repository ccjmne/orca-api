package org.ccjmne.orca.api.inject.business;

import java.sql.Date;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.api.utils.JSONFields;
import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.Nullable;
import org.jooq.Field;
import org.jooq.Param;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Provides tools to extract arguments from the current
 * {@link HttpServletRequest}.
 */
public class QueryParameters {

  public static final Type<Integer> EMPLOYEE              = new Type<>("employee", Integer.class);
  public static final Type<Integer> SITE                  = new Type<>("site", Integer.class);
  public static final Type<Integer> SESSION               = new Type<>("session", Integer.class);
  public static final Type<Integer> CERTIFICATE           = new Type<>("certificate", Integer.class);
  public static final Type<Boolean> INCLUDE_DECOMISSIONED = new Type<>("include-decommissioned", Boolean.class);
  public static final Type<String>  SEARCH_TERMS          = new Type<>("q", String.class);

  public static final FirstParamType<String>          INTERVAL       = new FirstParamType<>("interval", v -> v, "month");
  public static final FirstParamType<Field<Date>>     FROM           = new FirstParamType<>("from", v -> DSL.val(v, Date.class), DSL.currentDate());
  public static final FirstParamType<Field<Date>>     TO             = new FirstParamType<>("to", v -> DSL.val(v, Date.class), DSL.currentDate());
  public static final FirstParamType<Field<Date>>     DATE           = new FirstParamType<>("date", v -> DSL.val(v, Date.class), DSL.currentDate());
  public static final FirstParamType<Field<JsonNode>> GROUP_BY_FIELD = new FirstParamType<>("group-by", v -> DSL
      .field("site_tags -> {0}", JSONFields.JSON_TYPE, v), JSONFields.toJson(DSL.cast(Constants.TAGS_VALUE_UNIVERSAL, SQLDataType.VARCHAR)));

  private final Map<Type<?>, Param<?>>        types;
  private final Map<CustomType<?, ?>, Object> customTypes;

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
  public QueryParameters(@Context final UriInfo uriInfo) {
    this.types = Stream.concat(uriInfo.getQueryParameters().entrySet().stream(), uriInfo.getPathParameters().entrySet().stream())
        .map(Type::mapper).filter(Optional::isPresent).map(Optional::get)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (prev, next) -> next));
    this.customTypes = Stream.concat(uriInfo.getQueryParameters().entrySet().stream(), uriInfo.getPathParameters().entrySet().stream())
        .map(CustomType::mapper).filter(Optional::isPresent).map(Optional::get)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (prev, next) -> next));
  }

  public <T> Optional<Param<T>> of(final Type<T> type) {
    return Optional.ofNullable(this.get(type));
  }

  public <T> boolean has(final Type<T> type) {
    return this.of(type).isPresent() && (this.getRaw(type) != null);
  }

  public <T> boolean is(final Type<T> type, @NonNull final T value) {
    return this.has(type) && value.equals(this.getRaw(type));
  }

  @SuppressWarnings("unchecked") // always safe
  public <T> Param<T> get(final Type<T> type) {
    return (Param<T>) this.types.get(type);
  }

  public <T> T getRaw(final Type<T> type) {
    return this.get(type).getValue();
  }

  public <T> Optional<T> of(final CustomType<?, T> type) {
    return Optional.ofNullable(this.get(type));
  }

  /**
   * Always {@code true} for {@code CustomType}s that have a default value.
   *
   * @see QueryParameters#isDefault(CustomType)
   */
  public <T> boolean has(final CustomType<?, T> type) {
    return this.of(type).isPresent();
  }

  /**
   * Is {@code true} when the given {@code type} does have a default value that
   * either matches or wasn't overridden by the query parameters.
   */
  public <T> boolean isDefault(final CustomType<?, T> type) {
    return (type.orElse != null) && !this.customTypes.containsKey(type);
  }

  @SuppressWarnings("unchecked") // always safe
  public <T> T get(final CustomType<?, T> type) {
    return (T) this.customTypes.getOrDefault(type, type.orElse);
  }

  // TODO: Document
  // TODO: Rename?
  // TODO: extend CustomType?
  private static class Type<T> {

    private static final Map<String, Type<?>> TYPES = new HashMap<>();

    private final Class<T> type;

    protected Type(final String name, final Class<T> type) {
      this.type = type;
      TYPES.put(name, this);
    }

    protected static Optional<Map.Entry<Type<?>, Param<?>>> mapper(final Map.Entry<String, List<String>> source) {
      final Type<?> t = TYPES.get(source.getKey());
      return t != null ? Optional.of(new SimpleEntry<>(t, t.coerce(source.getValue().get(0)))) : Optional.empty();
    }

    private Param<T> coerce(final String value) {
      return DSL.val(value, this.type);
    }
  }

  /**
   * Like a {@link Type}, except that:
   * <ul>
   * <li>it can coerce parameters to any arbitrary {@code T} class</li>
   * <li>it can have a default value</li>
   * </ul>
   *
   * @param <U>
   *          For internal use only.
   * @param <T>
   *          The coerced value for this {@code CustomType}
   * @see FirstParamType
   * @see AllParamsType
   */
  // TODO: Rename?
  private static abstract class CustomType<U, T> {

    private static final Map<String, CustomType<?, ?>> TYPES = new HashMap<>();

    protected final @Nullable T orElse;

    private final Function<? super U, ? extends T> coercer;

    protected CustomType(final String name, final Function<? super U, ? extends T> coercer, @Nullable final T orElse) {
      this.coercer = coercer;
      this.orElse = orElse;
      // TODO: throw if replacing existing entry
      TYPES.put(name, this);
    }

    protected static <U, T> Optional<Map.Entry<CustomType<U, T>, T>> mapper(final Map.Entry<String, List<String>> source) {
      @SuppressWarnings("unchecked") // always safe
      final CustomType<U, T> t = (CustomType<U, T>) TYPES.get(source.getKey());
      return t != null ? Optional.of(new SimpleEntry<>(t, t.coercer.apply(t.supplyCoercer(source.getValue())))) : Optional.empty();
    }

    protected abstract U supplyCoercer(final List<String> parameters);
  }

  /**
   * A {@link CustomType} that consumes all the matching query parameters as a
   * {@code List<String>}.
   *
   * @param <T>
   *          The coerced value for this {@code CustomType}
   */
  private static class AllParamsType<T> extends CustomType<List<String>, T> {

    protected AllParamsType(final String name, final Function<? super List<String>, ? extends T> coercer, final @Nullable T orElse) {
      super(name, coercer, orElse);
    }

    @Override
    protected List<String> supplyCoercer(final List<String> parameters) {
      return parameters;
    }
  }

  /**
   * A {@link CustomType} that only considers the first matching query parameter.
   *
   * @param <T>
   *          The coerced value for this {@code CustomType}
   */
  private static class FirstParamType<T> extends CustomType<String, T> {

    protected FirstParamType(final String name, final Function<String, ? extends T> coercer, final @Nullable T orElse) {
      super(name, coercer, orElse);
    }

    @Override
    protected String supplyCoercer(final List<String> parameters) {
      return parameters.get(0);
    }
  }
}
