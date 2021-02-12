package com.transaction.error.transaction_error;

import io.reactivex.Completable;
import io.reactivex.Single;
import io.vertx.core.*;
import io.vertx.reactivex.impl.AsyncResultCompletable;
import io.vertx.reactivex.impl.AsyncResultSingle;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.ResultCursor;

import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

public class Transaction {

  public static Transaction create(AsyncTransaction tx, AsyncSession session, Vertx vertx) {
    return new Transaction(tx, session, vertx);
  }

  private final Vertx vertx;
  private final AsyncTransaction tx;
  private final AsyncSession session;

  private Transaction(AsyncTransaction tx, AsyncSession session, Vertx vertx) {
    this.tx = tx;
    this.vertx = vertx;
    this.session = session;
  }

  public Single<List<Record>> query(Query statement) {
    return AsyncResultSingle.toSingle(handler -> query(statement, handler));
  }

  private Transaction query(Query query, Handler<AsyncResult<List<Record>>> resultHandler) {
    Context context = vertx.getOrCreateContext();

    tx.runAsync(query)
      .thenCompose(ResultCursor::listAsync)
      .thenAccept(records -> context.runOnContext(v -> resultHandler.handle(Future.succeededFuture(records))))
      .exceptionally(error -> {
        context.runOnContext(v -> resultHandler.handle(Future.failedFuture(Optional.ofNullable(error.getCause()).orElse(error))));
        return null;
      });
    return this;
  }

  public Completable commit() {
    return AsyncResultCompletable.toCompletable(this::commit);
  }

  private Transaction commit(Handler<AsyncResult<Void>> resultHandler) {
    Context context = vertx.getOrCreateContext();
    tx.commitAsync().whenComplete(wrapCallback(context, resultHandler))
      .thenCompose(ignore -> session.closeAsync());
    return this;
  }

  public <T> BiConsumer<T, Throwable> wrapCallback(Context context, Handler<AsyncResult<T>> resultHandler) {
    return (result, error) -> context.runOnContext(v -> {
      if (error != null) {
        resultHandler.handle(Future.failedFuture(Optional.ofNullable(error.getCause()).orElse(error)));
      } else {
        resultHandler.handle(Future.succeededFuture(result));
      }
    });
  }
}
