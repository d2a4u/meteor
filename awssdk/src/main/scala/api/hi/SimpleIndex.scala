package meteor
package api.hi

import fs2.{Pipe, RaiseThrowable}
import cats.implicits._
import cats.effect.Async
import meteor.api._
import meteor.codec.{Decoder, Encoder}
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.ReturnValue

import scala.concurrent.duration.FiniteDuration

abstract class SimpleIndex[F[_]: Async, P: Encoder] extends PartitionKeyGetOps {
  def partitionKeyDef: KeyDef[P]
  def jClient: DynamoDbAsyncClient

  def index: PartitionKeyIndex[P]

  def retrieve[T: Decoder](
    query: Query[P, Nothing],
    consistentRead: Boolean
  )(implicit RT: RaiseThrowable[F]): F[Option[T]] =
    retrieveOp[F, P, T](
      index,
      query,
      consistentRead
    )(jClient)
}

case class SecondarySimpleIndex[F[_]: Async, P: Encoder](
  tableName: String,
  indexName: String,
  partitionKeyDef: KeyDef[P],
  jClient: DynamoDbAsyncClient
) extends SimpleIndex[F, P] {
  val index: PartitionKeyIndex[P] =
    PartitionKeySecondaryIndex[P](tableName, indexName, partitionKeyDef)
}

case class SimpleTable[F[_]: Async, P: Encoder](
  tableName: String,
  partitionKeyDef: KeyDef[P],
  jClient: DynamoDbAsyncClient
) extends SimpleIndex[F, P]
    with PutOps
    with PartitionKeyDeleteOps
    with PartitionKeyUpdateOps
    with PartitionKeyBatchGetOps
    with PartitionKeyBatchWriteOps {
  private val table: PartitionKeyTable[P] =
    PartitionKeyTable[P](tableName, partitionKeyDef)

  val index: PartitionKeyIndex[P] = table

  def get[T: Decoder](
    partitionKey: P,
    consistentRead: Boolean
  ): F[Option[T]] =
    getOp[F, P, T](table, partitionKey, consistentRead)(jClient)

  /** Put an item into a table, return ReturnValue.NONE.
    */
  def put[T: Encoder](
    t: T,
    condition: Expression = Expression.empty
  ): F[Unit] =
    putOp[F, T](table.tableName, t, condition)(jClient)

  /** Put an item into a table, return ReturnValue.ALL_OLD.
    */
  def put[T: Encoder, U: Decoder](
    t: T,
    condition: Expression
  ): F[Option[U]] =
    putOp[F, T, U](table.tableName, t, condition)(jClient)

  def delete(partitionKey: P): F[Unit] =
    deleteOp[F, P, Unit](table, partitionKey, ReturnValue.NONE)(jClient).void

  /** Update an item by partition key P given an update expression
    * when it fulfills a condition expression.
    * Return Unit (ReturnValue.NONE).
    */
  def update(
    partitionKey: P,
    update: Expression,
    condition: Expression = Expression.empty
  ): F[Unit] =
    updateOp[F, P](table, partitionKey, update, condition)(
      jClient
    )

  /** Update an item by partition key P given an update expression
    * when it fulfills a condition expression.
    * A Codec of U is required to deserialize return value.
    */
  def update[T: Decoder](
    partitionKey: P,
    returnValue: ReturnValue,
    update: Expression,
    condition: Expression
  ): F[Option[T]] =
    updateOp[F, P, T](table, partitionKey, update, condition, returnValue)(
      jClient
    )

  def batchGet[T: Decoder](
    consistentRead: Boolean,
    projection: Expression,
    maxBatchWait: FiniteDuration,
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  ): Pipe[F, P, T] =
    batchGetOp[F, P, T](
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
  ): Pipe[F, T, Unit] =
    batchPutInorderedOp[F, T](table, maxBatchWait, backoffStrategy)(jClient)

  /** Batch put items into a table where ordering of input items does not matter
    */
  def batchPutUnordered[T: Encoder](
    maxBatchWait: FiniteDuration,
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  ): Pipe[F, T, Unit] =
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
  ): Pipe[F, P, Unit] =
    batchDeleteUnorderedOp[F, P](
      table,
      maxBatchWait,
      parallelism,
      backoffStrategy
    )(jClient)

  def batchWrite[T: Encoder](
    maxBatchWait: FiniteDuration,
    backoffStrategy: BackoffStrategy
  ): Pipe[F, Either[P, T], Unit] =
    batchWriteInorderedOp[F, P, T](table, maxBatchWait, backoffStrategy)(
      jClient
    )
}
