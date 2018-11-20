package org.ccjmne.orca.api.utils;

import java.util.function.Consumer;
import java.util.function.Function;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;

// TODO: Singleton instance with its own DSLContext injected
public class Transactions {

  public static void with(final DSLContext ctx, final TransactionConsumer<DSLContext> task) {
    ctx.transaction(config -> {
      try (final DSLContext transactionCtx = DSL.using(config)) {
        task.accept(transactionCtx);
      }
    });
  }

  public static <T> T with(final DSLContext ctx, final TransactionFunction<DSLContext, T> task) {
    return ctx.transactionResult(config -> {
      try (final DSLContext transactionCtx = DSL.using(config)) {
        return task.apply(transactionCtx);
      }
    });
  }

  @FunctionalInterface
  public interface TransactionConsumer<T> extends Consumer<T> {

    @Override
    default void accept(final T t) {
      try {
        this.acceptThrows(t);
      } catch (final RuntimeException e) {
        throw e;
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    void acceptThrows(T t) throws Exception;
  }

  @FunctionalInterface
  public interface TransactionFunction<T, R> extends Function<T, R> {

    @Override
    default R apply(final T t) {
      try {
        return this.applyThrows(t);
      } catch (final RuntimeException e) {
        throw e;
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    R applyThrows(T t) throws Exception;
  }
}
