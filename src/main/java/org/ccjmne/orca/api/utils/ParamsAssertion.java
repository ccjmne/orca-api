package org.ccjmne.orca.api.utils;

import static org.ccjmne.orca.jooq.codegen.Tables.CERTIFICATES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGTYPES;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Map.Entry;

import javax.activation.UnsupportedDataTypeException;
import javax.inject.Inject;

import org.ccjmne.orca.api.inject.business.QueryParams;
import org.ccjmne.orca.api.inject.business.QueryParams.Type;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.DSL;

import com.google.common.collect.ImmutableMap;
import com.mchange.util.AssertException;

/**
 * Centralises prerequisite assertions about the current database state to
 * assist in performing context-aware validation of query parameters.
 *
 * @author ccjmne
 */
// TODO: Should it be absorbed by QueryParams?
public class ParamsAssertion {

  private static Map<Type<?, ? extends Field<Integer>>, Entry<Table<?>, TableField<?, Integer>>> FIELDS = ImmutableMap
      .of(QueryParams.CERTIFICATE, new SimpleEntry<>(CERTIFICATES, CERTIFICATES.CERT_PK),
          QueryParams.SESSION_TYPE, new SimpleEntry<>(TRAININGTYPES, TRAININGTYPES.TRTY_PK));

  private final QueryParams parameters;

  @Inject
  public ParamsAssertion(final QueryParams parameters) {
    this.parameters = parameters;
  }

  public AssertionResult resourceExists(final Type<?, ? extends Field<Integer>> type, final DSLContext transactionCtx) {
    final Entry<Table<?>, TableField<?, Integer>> fields = ParamsAssertion.FIELDS.get(type);
    if (fields == null) {
      throw new IllegalArgumentException("Could not ensure existence of resource", new UnsupportedDataTypeException("Unsupported QueryParams.Type"));
    }

    return new AssertionResult(transactionCtx.fetchExists(DSL.selectFrom(fields.getKey()).where(fields.getValue().eq(this.parameters.get(type)))));
  }

  public class AssertionResult {

    final boolean passed;

    public AssertionResult(final boolean passed) {
      this.passed = passed;
    }

    public boolean hasPassed() {
      return this.passed;
    }

    public void or(final String message) {
      if (!this.passed) {
        throw new AssertException(message);
      }
    }
  }
}
