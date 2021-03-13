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

abstract class SimpleIndex[F[_], P: Encoder] extends PartitionKeyGetOps {
  def partitionKeyDef: KeyDef[P]
  def jClient: DynamoDbAsyncClient

  def index: PartitionKeyIndex[P]

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

case class SecondarySimpleIndex[F[_], P: Encoder](
  tableName: String,
  indexName: String,
  partitionKeyDef: KeyDef[P],
  jClient: DynamoDbAsyncClient
) extends SimpleIndex[F, P] {
  val index: PartitionKeyIndex[P] =
    PartitionKeySecondaryIndex[P](tableName, indexName, partitionKeyDef)
}

case class SimpleTable[F[_], P: Encoder](
  tableName: String,
  partitionKeyDef: KeyDef[P],
  jClient: DynamoDbAsyncClient
) extends SimpleIndex[F, P]
    with PutOps
    with PartitionKeyDeleteOps
    with PartitionKeyUpdateOps
    with PartitionKeyBatchGetOps
    with PartitionKeyBatchWriteOps {
  protected val index = PartitionKeyTable[P](tableName, partitionKeyDef)

  def get[T: Decoder](
    partitionKey: P,
    consistentRead: Boolean
  )(implicit F: Concurrent[F]): F[Option[T]] =
    getOp[F, P, T](index, partitionKey, consistentRead)(jClient)

  /**
    * Put an item into a table, return ReturnValue.NONE.
    */
  def put[T: Encoder](
    t: T,
    condition: Expression = Expression.empty
  )(implicit F: Concurrent[F]): F[Unit] =
    putOp[F, T](index.tableName, t, condition)(jClient)

  /**
    * Put an item into a table, return ReturnValue.ALL_OLD.
    */
  def put[T: Encoder, U: Decoder](
    t: T,
    condition: Expression
  )(implicit F: Concurrent[F]): F[Option[U]] =
    putOp[F, T, U](index.tableName, t, condition)(jClient)

  def delete(partitionKey: P)(implicit F: Concurrent[F]): F[Unit] =
    deleteOp[F, P](index, partitionKey)(jClient)

  /**
    * Update an item by partition key P given an update expression
    * when it fulfills a condition expression.
    * Return Unit (ReturnValue.NONE).
    */
  def update(
    partitionKey: P,
    update: Expression,
    condition: Expression = Expression.empty
  )(implicit F: Concurrent[F]): F[Unit] =
    updateOp[F, P](index, partitionKey, update, condition)(
      jClient
    )

  /**
    * Update an item by partition key P given an update expression
    * when it fulfills a condition expression.
    * A Codec of U is required to deserialize return value.
    */
  def update[T: Decoder](
    partitionKey: P,
    returnValue: ReturnValue,
    update: Expression,
    condition: Expression
  )(implicit F: Concurrent[F]): F[Option[T]] =
    updateOp[F, P, T](index, partitionKey, update, condition, returnValue)(
      jClient
    )

  def batchGet[T: Decoder](
    consistentRead: Boolean,
    projection: Expression,
    maxBatchWait: FiniteDuration,
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  )(implicit F: Concurrent[F], TI: Timer[F]): Pipe[F, P, T] =
    batchGetOp[F, P, T](
      index,
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
    batchPutInorderedOp[F, T](index, maxBatchWait, backoffStrategy)(jClient)

  /**
    * Batch put items into a table where ordering of input items does not matter
    */
  def batchPutUnordered[T: Encoder](
    maxBatchWait: FiniteDuration,
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  )(implicit F: Concurrent[F], TI: Timer[F]): Pipe[F, T, Unit] =
    batchPutUnorderedOp[F, T](
      index.tableName,
      maxBatchWait,
      parallelism,
      backoffStrategy
    )(jClient)

  def batchDelete(
    maxBatchWait: FiniteDuration,
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  )(implicit F: Concurrent[F], TI: Timer[F]): Pipe[F, P, Unit] =
    batchDeleteUnorderedOp[F, P](
      index,
      maxBatchWait,
      parallelism,
      backoffStrategy
    )(jClient)

  def batchWrite[T: Encoder](
    maxBatchWait: FiniteDuration,
    backoffStrategy: BackoffStrategy
  )(implicit F: Concurrent[F], TI: Timer[F]): Pipe[F, Either[P, T], Unit] =
    batchWriteInorderedOp[F, P, T](index, maxBatchWait, backoffStrategy)(
      jClient
    )
}
