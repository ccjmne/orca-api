package org.ccjmne.orca.api.utils;

import java.util.function.Consumer;
import java.util.function.Function;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;

// TODO: use this everywhere
public class Transactions {

  public static void with(final DSLContext ctx, final Consumer<DSLContext> task) {
    ctx.transaction(config -> {
      try (final DSLContext transactionCtx = DSL.using(config)) {
        task.accept(transactionCtx);
      } catch (final Exception e) {
        throw new IllegalArgumentException(e);
      }
    });
  }

  public static <T> T with(final DSLContext ctx, final Function<DSLContext, T> task) {
    return ctx.transactionResult(config -> {
      try (final DSLContext transactionCtx = DSL.using(config)) {
        return task.apply(transactionCtx);
      } catch (final Exception e) {
        throw new IllegalArgumentException(e);
      }
    });
  }
}
