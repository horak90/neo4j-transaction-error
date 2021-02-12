package com.transaction.error.transaction_error;

import io.reactivex.Single;
import io.vertx.core.*;
import io.vertx.reactivex.impl.AsyncResultSingle;
import org.neo4j.driver.*;
import org.neo4j.driver.async.AsyncSession;

import static org.neo4j.driver.AccessMode.WRITE;
import static org.neo4j.driver.Values.parameters;

public class MainVerticle extends AbstractVerticle {

  private static final SessionConfig DEFAULT_WRITE_SESSION_CONFIG = SessionConfig.builder().withDefaultAccessMode(WRITE).build();

  public static final String GET_LEFT_OR_RIGHT_NODE = "MATCH (n:NODE) WHERE n.id = $left OR n.id = $right RETURN (n)";

  public static final String SET_CONSTRAINT = "CREATE CONSTRAINT id_property IF NOT EXISTS ON (p:NODE) ASSERT p.id IS UNIQUE";

  public static final String DELETE_DB = "MATCH (n) DETACH DELETE n;";

  public static final String CREATE_TEST_DATA = "CREATE (:NODE {id:'1'}), (:NODE {id:'2', relationProp:2});";

  @Override
  public void start(Promise<Void> startPromise) {
    Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "2012-ble"));

    beginTransaction(driver)
      .flatMapCompletable(tx ->
          tx.query(new Query(SET_CONSTRAINT)).flatMapCompletable(r -> tx.commit())
      )
      .andThen(Single.defer(() -> beginTransaction(driver)))
      .flatMapCompletable(tx ->
          tx.query(new Query(DELETE_DB)).flatMapCompletable(r -> tx.commit())
      )
      .andThen(Single.defer(() -> beginTransaction(driver)))
      .flatMapCompletable(tx ->
          tx.query(new Query(CREATE_TEST_DATA))
            .flatMap(i -> tx.query(new Query(GET_LEFT_OR_RIGHT_NODE, parameters("left", "1", "right", "3"))))
            .flatMapCompletable(r -> tx.commit())
      )
      .subscribe(
          () -> {
            startPromise.complete();
            System.out.println("The transaction should not success");
          },
          err -> {
            startPromise.complete();
            System.out.println("transaction failed with the following error : " + err);
          }
      );
  }

  private Single<Transaction> beginTransaction(Driver driver) {
    return AsyncResultSingle.toSingle(handler -> beginTransaction(driver, handler));
  }

  private void beginTransaction(Driver driver, Handler<AsyncResult<Transaction>> resultHandler) {
    AsyncSession session = driver.asyncSession(DEFAULT_WRITE_SESSION_CONFIG);
    Context context = vertx.getOrCreateContext();

    session.beginTransactionAsync()
      .thenAccept(tx -> context.runOnContext(v -> resultHandler.handle(Future.succeededFuture(Transaction.create(tx, session, vertx)))))
      .exceptionally(error -> {
          context.runOnContext(v -> resultHandler.handle(Future.failedFuture(error)));
          session.closeAsync();
          return null;
      });
  }
}
