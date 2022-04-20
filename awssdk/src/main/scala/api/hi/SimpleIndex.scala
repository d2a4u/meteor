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

private[meteor] sealed abstract class SimpleIndex[F[_]: Async, P: Encoder]
    extends PartitionKeyGetOps {
  def partitionKeyDef: KeyDef[P]
  def jClient: DynamoDbAsyncClient

  def index: PartitionKeyIndex[P]

  /** Retrieve items from a partition key index, can be a secondary index or a table which has only
    * partition key and no sort key.
    *
    * @param query a query to filter items by key condition
    * @param consistentRead toggle to perform consistent read
    * @param RT implicit evidence for RaiseThrowable
    * @tparam T return item's type
    * @return optional item of type T
    */
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

/** Represent a secondary index where the index has only partition key and no sort key.
  *
  * @param tableName table's name
  * @param indexName index's name
  * @param partitionKeyDef partition key definition
  * @param jClient DynamoDB java async client
  * @tparam F effect type
  * @tparam P partition key type
  */
@deprecated(
  "use meteor.hi.GlobalSecondarySimpleIndex instead - see https://github.com/d2a4u/meteor/issues/229 for more details",
  "2022-04-21"
)
case class SecondarySimpleIndex[F[_]: Async, P: Encoder](
  tableName: String,
  indexName: String,
  partitionKeyDef: KeyDef[P],
  jClient: DynamoDbAsyncClient
) extends SimpleIndex[F, P] {
  val index: PartitionKeyIndex[P] =
    PartitionKeySecondaryIndex[P](tableName, indexName, partitionKeyDef)
}

/** Represent a table where the index has only partition key and no sort key.
  *
  * @param tableName table's name
  * @param partitionKeyDef partition key definition
  * @param jClient DynamoDB java async client
  * @tparam F effect type
  * @tparam P partition key's type
  */
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

  /** Get a single item by partition key.
    *
    * @param partitionKey partition key
    * @param consistentRead flag to enable strongly consistent read
    * @tparam T returned item's type
    * @return an optional item of type T
    */
  def get[T: Decoder](
    partitionKey: P,
    consistentRead: Boolean
  ): F[Option[T]] =
    getOp[F, P, T](table, partitionKey, consistentRead)(jClient)

  /** Put an item into a table.
    *
    * @param t item to be put
    * @param condition conditional expression
    * @tparam T item's type
    * @return Unit
    */
  def put[T: Encoder](
    t: T,
    condition: Expression = Expression.empty
  ): F[Unit] =
    putOp[F, T](table.tableName, t, condition)(jClient)

  /** Put an item into a table and return previous value.
    *
    * @param t item to be put
    * @param condition conditional expression
    * @tparam T item's type
    * @tparam U returned item's type
    * @return an option item of type U
    */
  def put[T: Encoder, U: Decoder](
    t: T,
    condition: Expression
  ): F[Option[U]] =
    putOp[F, T, U](table.tableName, t, condition)(jClient)

  /** Delete an item by partition key.
    *
    * @param partitionKey partition key
    * @return Unit
    */
  def delete(partitionKey: P): F[Unit] =
    deleteOp[F, P, Unit](table, partitionKey, ReturnValue.NONE)(jClient).void

  /** Update an item by partition key given an update expression when a condition expression is
    * fulfilled. Return Unit.
    *
    * @param partitionKey partition key
    * @param update update expression
    * @param condition conditional expression
    * @return Unit
    */
  def update(
    partitionKey: P,
    update: Expression,
    condition: Expression = Expression.empty
  ): F[Unit] =
    updateOp[F, P](table, partitionKey, update, condition)(
      jClient
    )

  /** Update an item by partition key given an update expression when a condition expression is
    * fulfilled. Return item is customizable via `returnValue` parameter.
    *
    * @param partitionKey partition key
    * @param returnValue flag to define which item to be returned
    * @param update update expression
    * @param condition conditional expression
    * @tparam T returned item's type
    * @return an optional item of type T
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

  /** Get items by partition key in batch. Max batch size is preset to 100 (maximum batch size
    * permitted for batch get action), however, it is possible to control the rate of batching with
    * `maxBatchWait` parameter. This uses fs2 .groupWithin internally. This returns a Pipe in order to
    * cover broader use cases regardless of input can be fitted into memory or not. Duplicated items
    * within a batch will be removed. Left over items within a batch will be reprocessed in the next
    * batch.
    *
    * @param consistentRead flag to enable strongly consistent read
    * @param projection projection expression
    * @param maxBatchWait time window to collect items into a batch
    * @param parallelism number of connections that can be open at the same time
    * @param backoffStrategy backoff strategy in case of failure, default can be found at
    *                        [[meteor.Client.BackoffStrategy.default]].
    * @tparam T returned item's type
    * @return a fs2 Pipe from partition key P to T
    */
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

  /** Put items in batch, '''in ordered'''. Meaning batches are processed in '''serial''' to avoid
    * race condition when writing items with the same partition key. If your input doesn't have
    * this constrain, you can use [[batchPutUnordered]]. Max batch size is preset to 25 (maximum
    * batch size permitted from batch put actions), however, it is possible to control the rate of
    * batching with `maxBatchWait` parameter. This uses fs2 .groupWithin internally. This returns a
    * Pipe in order to cover broader use cases regardless of input can be fitted into memory or not.
    * Duplicated items within a batch will be removed. Left over items within a batch will be
    * reprocessed in the next batch.
    *
    * @param maxBatchWait time window to collect items into a batch
    * @param backoffStrategy backoff strategy in case of failure, default can be found at
    *                        [[meteor.Client.BackoffStrategy.default]].
    * @tparam T returned item's type
    * @return a fs2 Pipe from T to Unit
    */
  def batchPut[T: Encoder](
    maxBatchWait: FiniteDuration,
    backoffStrategy: BackoffStrategy
  ): Pipe[F, T, Unit] =
    batchPutInorderedOp[F, T](table, maxBatchWait, backoffStrategy)(jClient)

  /** Put items in batch, '''un-ordered'''. Meaning batches are processed in '''parallel''', hence,
    * if your input has items with the same partition key, this can cause a race condition,
    * consider using [[batchPut]] instead. Max batch size is preset to 25 (maximum batch
    * size permitted from batch put actions), however, it is possible to control the rate of
    * batching with `maxBatchWait` parameter. This uses fs2 .groupWithin internally. This returns a
    * Pipe in order to cover broader use cases regardless of input can be fitted into memory or not.
    * Duplicated items within a batch will be removed. Left over items within a batch will be
    * reprocessed in the next batch.
    *
    * @param maxBatchWait time window to collect items into a batch
    * @param parallelism number of connections that can be open at the same time
    * @param backoffStrategy backoff strategy in case of failure, default can be found at
    *                        [[meteor.Client.BackoffStrategy.default]].
    * @tparam T returned item's type
    * @return a fs2 Pipe from T to Unit
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

  /** Delete items by partition key in batch. Max batch size is preset to 100 (maximum batch size
    * permitted for batch get action), however, it is possible to control the rate of batching with
    * `maxBatchWait` parameter. This uses fs2 .groupWithin internally. This returns a Pipe in order
    * to cover broader use cases regardless of input can be fitted into memory or not. Duplicated
    * items within a batch will be removed. Left over items within a batch will be reprocessed in
    * the next batch.
    *
    * @param maxBatchWait time window to collect items into a batch
    * @param parallelism number of connections that can be open at the same time
    * @param backoffStrategy backoff strategy in case of failure, default can be found at
    *                        [[meteor.Client.BackoffStrategy.default]].
    * @return a fs2 Pipe from composite keys P and S as a tuple to Unit
    */
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

  /** Write items (put or delete) in batch, '''in ordered'''. Meaning batches are processed in
    * '''serial''' to avoid race condition when writing items with the same partition key. Max
    * batch size is preset to 25 (maximum batch size permitted from batch write actions), however,
    * it is possible to control the rate of batching with `maxBatchWait` parameter. This uses fs2
    * .groupWithin internally. This returns a Pipe in order to cover broader use cases regardless of
    * input can be fitted into memory or not. Duplicated items within a batch will be removed. Left
    * over items within a batch will be reprocessed in the next batch.
    *
    * @param maxBatchWait time window to collect items into a batch
    * @param backoffStrategy backoff strategy in case of failure, default can be found at
    *                        [[meteor.Client.BackoffStrategy.default]].
    * @tparam T returned item's type
    * @return a fs2 Pipe from Either[P, T], represent deletion (Left) or put (Right) to Unit.
    */
  def batchWrite[T: Encoder](
    maxBatchWait: FiniteDuration,
    backoffStrategy: BackoffStrategy
  ): Pipe[F, Either[P, T], Unit] =
    batchWriteInorderedOp[F, P, T](table, maxBatchWait, backoffStrategy)(
      jClient
    )
}
