package meteor
package api.hi

import cats.effect.{Concurrent, Timer}
import fs2.{Pipe, RaiseThrowable}
import meteor.api._
import meteor.codec.{Decoder, Encoder}
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.ReturnValue

import scala.concurrent.duration.FiniteDuration

abstract class CompositeIndex[F[_], P: Encoder, S: Encoder]
    extends CompositeKeysGetOps {
  def partitionKeyDef: KeyDef[P]
  def sortKeyDef: KeyDef[S]
  def jClient: DynamoDbAsyncClient

  def index: CompositeKeysIndex[P, S]

  def retrieveOp[T: Decoder](
    partitionKey: P,
    consistentRead: Boolean,
    limit: Int
  )(implicit F: Concurrent[F], RT: RaiseThrowable[F]): fs2.Stream[F, T] =
    retrieveOp[F, P, T](
      index,
      partitionKey,
      consistentRead,
      limit
    )(jClient)
}

case class SecondaryCompositeIndex[F[_], P: Encoder, S: Encoder](
  tableName: String,
  indexName: String,
  partitionKeyDef: KeyDef[P],
  sortKeyDef: KeyDef[S],
  jClient: DynamoDbAsyncClient
) extends CompositeIndex[F, P, S] {
  val index: CompositeKeysIndex[P, S] =
    CompositeKeysSecondaryIndex[P, S](
      tableName,
      indexName,
      partitionKeyDef,
      sortKeyDef
    )
}

case class CompositeTable[F[_], P: Encoder, S: Encoder](
  tableName: String,
  partitionKeyDef: KeyDef[P],
  sortKeyDef: KeyDef[S],
  jClient: DynamoDbAsyncClient
) extends CompositeIndex[F, P, S]
    with PutOps
    with CompositeKeysDeleteOps
    with CompositeKeysUpdateOps
    with CompositeKeysBatchGetOps
    with CompositeKeysBatchWriteOps {
  private val table =
    CompositeKeysTable[P, S](tableName, partitionKeyDef, sortKeyDef)

  val index: CompositeKeysIndex[P, S] = table

  def get[T: Decoder](
    partitionKey: P,
    sortKey: S,
    consistentRead: Boolean
  )(implicit F: Concurrent[F]): F[Option[T]] =
    getOp[F, P, S, T](table, partitionKey, sortKey, consistentRead)(jClient)

  /**
    * Put an item into a table, return ReturnValue.NONE.
    */
  def put[T: Encoder](
    t: T,
    condition: Expression = Expression.empty
  )(implicit F: Concurrent[F]): F[Unit] =
    putOp[F, T](table.tableName, t, condition)(jClient)

  /**
    * Put an item into a table, return ReturnValue.ALL_OLD.
    */
  def put[T: Encoder, U: Decoder](
    t: T,
    condition: Expression
  )(implicit F: Concurrent[F]): F[Option[U]] =
    putOp[F, T, U](table.tableName, t, condition)(jClient)

  def delete(partitionKey: P, sortKey: S)(implicit F: Concurrent[F]): F[Unit] =
    deleteOp[F, P, S](table, partitionKey, sortKey)(jClient)

  /**
    * Update an item by partition key P given an update expression
    * when it fulfills a condition expression.
    * Return Unit (ReturnValue.NONE).
    */
  def update(
    partitionKey: P,
    sortKey: S,
    update: Expression,
    condition: Expression = Expression.empty
  )(implicit F: Concurrent[F]): F[Unit] =
    updateOp[F, P, S](table, partitionKey, sortKey, update, condition)(
      jClient
    )

  /**
    * Update an item by partition key P given an update expression
    * when it fulfills a condition expression.
    * A Codec of U is required to deserialize return value.
    */
  def update[T: Decoder](
    partitionKey: P,
    sortKey: S,
    returnValue: ReturnValue,
    update: Expression,
    condition: Expression
  )(implicit F: Concurrent[F]): F[Option[T]] =
    updateOp[F, P, S, T](
      table,
      partitionKey,
      sortKey,
      update,
      condition,
      returnValue
    )(
      jClient
    )

  def batchGet[T: Decoder](
    consistentRead: Boolean,
    projection: Expression,
    maxBatchWait: FiniteDuration,
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  )(implicit F: Concurrent[F], TI: Timer[F]): Pipe[F, (P, S), T] =
    batchGetOp[F, P, S, T](
      table,
      consistentRead,
      projection,
      maxBatchWait,
      parallelism,
      backoffStrategy
    )(jClient)

  def batchPut[T: Encoder](
    maxBatchWait: FiniteDuration,
    backoffStrategy: BackoffStrategy
  )(implicit F: Concurrent[F], TI: Timer[F]): Pipe[F, T, Unit] =
    batchPutInorderedOp[F, T](table, maxBatchWait, backoffStrategy)(jClient)

  /**
    * Batch put items into a table where ordering of input items does not matter
    */
  def batchPutUnordered[T: Encoder](
    maxBatchWait: FiniteDuration,
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  )(implicit F: Concurrent[F], TI: Timer[F]): Pipe[F, T, Unit] =
    batchPutUnorderedOp[F, T](
      table.tableName,
      maxBatchWait,
      parallelism,
      backoffStrategy
    )(jClient)

  def batchDelete(
    maxBatchWait: FiniteDuration,
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  )(implicit F: Concurrent[F], TI: Timer[F]): Pipe[F, (P, S), Unit] =
    batchDeleteUnorderedOp[F, P, S](
      table,
      maxBatchWait,
      parallelism,
      backoffStrategy
    )(jClient)

  def batchWrite[T: Encoder](
    maxBatchWait: FiniteDuration,
    backoffStrategy: BackoffStrategy
  )(implicit F: Concurrent[F], TI: Timer[F]): Pipe[F, Either[(P, S), T], Unit] =
    batchWriteInorderedOp[F, P, S, T](table, maxBatchWait, backoffStrategy)(
      jClient
    )
}
