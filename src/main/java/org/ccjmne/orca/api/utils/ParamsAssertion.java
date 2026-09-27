package org.ccjmne.orca.api.utils;

import static org.ccjmne.orca.jooq.codegen.Tables.CERTIFICATES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGTYPES;

import java.util.Map;

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

  private static Map<Type<?, ? extends Field<Integer>>, AssertionConfig> FIELDS = ImmutableMap.of(
          QueryParams.CERTIFICATE,  new AssertionConfig("certificate",    CERTIFICATES,  CERTIFICATES.CERT_PK),
          QueryParams.SESSION_TYPE, new AssertionConfig("training type",  TRAININGTYPES, TRAININGTYPES.TRTY_PK));

  private final QueryParams params;

  @Inject
  public ParamsAssertion(final QueryParams params) {
    this.params = params;
  }

  public void resourceExists(final Type<?, ? extends Field<Integer>> type, final DSLContext tx) {
    final var cfg = ParamsAssertion.FIELDS.get(type);
    if (cfg == null) {
      throw new IllegalArgumentException("Could not ensure existence of resource", new UnsupportedDataTypeException("Unsupported QueryParams.Type"));
    }
    if (!tx.fetchExists(DSL.selectFrom(cfg.table).where(cfg.field.eq(this.params.get(type))))) {
      throw new AssertException(String.format("No %s for %d.", cfg.kind, this.params.get(type)));
    }
  }

  private static class AssertionConfig {

    private final String                 kind;
    private final Table<?>               table;
    private final TableField<?, Integer> field;

    private AssertionConfig(final String kind, final Table<?> table, final TableField<?, Integer> field) {
      this.kind  = kind;
      this.table = table;
      this.field = field;
    }
  }
}
