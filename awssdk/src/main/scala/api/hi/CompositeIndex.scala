package meteor
package api.hi

import cats.implicits._
import cats.effect.Async
import fs2.{Pipe, RaiseThrowable}
import meteor.api._
import meteor.codec.{Decoder, Encoder}
import meteor.errors.UnsupportedArgument
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.ReturnValue

import scala.concurrent.duration.FiniteDuration

private[meteor] sealed abstract class CompositeIndex[
  F[_]: Async,
  P: Encoder,
  S: Encoder
] extends CompositeKeysGetOps {
  def partitionKeyDef: KeyDef[P]
  def sortKeyDef: KeyDef[S]
  def jClient: DynamoDbAsyncClient

  def index: CompositeKeysIndex[P, S]

  /** Retrieve items from a composite index, can be a secondary index or a table which has composite
    * keys (partition key and sort key).
    *
    * @param query a query to filter items by key condition
    * @param consistentRead toggle to perform consistent read
    * @param limit limit the number of items to be returned per API call
    * @param RT implicit evidence for RaiseThrowable
    * @tparam T return item's type
    * @return a fs2 Stream of items
    */
  @deprecated("Use retrieve without limit", "2022-04-24")
  def retrieve[T: Decoder](
    query: Query[P, S],
    consistentRead: Boolean,
    limit: Int
  )(implicit RT: RaiseThrowable[F]): fs2.Stream[F, T] =
    retrieveOp[F, P, S, T](
      index,
      query,
      consistentRead,
      limit.some
    )(jClient)

  /** Retrieve items from a composite index, can be a secondary index or a table which has composite
    * keys (partition key and sort key).
    *
    * @param query a query to filter items by key condition
    * @param consistentRead toggle to perform consistent read
    * @param RT implicit evidence for RaiseThrowable
    * @tparam T return item's type
    * @return a fs2 Stream of items
    */
  def retrieve[T: Decoder](
    query: Query[P, S],
    consistentRead: Boolean
  )(implicit RT: RaiseThrowable[F]): fs2.Stream[F, T] =
    retrieveOp[F, P, S, T](
      index,
      query,
      consistentRead,
      limit = None
    )(jClient)
}

/** Represent a global secondary index where the index has only partition key and no sort key.
  *
  * @param tableName table's name
  * @param indexName index's name
  * @param partitionKeyDef partition key definition
  * @param jClient DynamoDB java async client
  * @tparam F effect type
  * @tparam P partition key type
  */
case class GlobalSecondarySimpleIndex[F[_]: Async, P: Encoder](
  tableName: String,
  indexName: String,
  partitionKeyDef: KeyDef[P],
  jClient: DynamoDbAsyncClient
) extends CompositeIndex[F, P, Nothing] {
  val sortKeyDef: KeyDef[Nothing] = KeyDef.nothing
  val index: CompositeKeysIndex[P, Nothing] =
    CompositeKeysSecondaryIndex[P, Nothing](
      tableName,
      indexName,
      partitionKeyDef,
      sortKeyDef
    )

  @deprecated("Use retrieve without limit", "2022-04-24")
  override def retrieve[T: Decoder](
    query: Query[P, Nothing],
    consistentRead: Boolean,
    limit: Int
  )(implicit RT: RaiseThrowable[F]): fs2.Stream[F, T] = {
    if (consistentRead) {
      fs2.Stream.raiseError(
        UnsupportedArgument("Consistent read is not supported")
      )
    } else {
      super.retrieve(query, consistentRead, limit)
    }
  }

  override def retrieve[T: Decoder](
    query: Query[P, Nothing],
    consistentRead: Boolean
  )(implicit RT: RaiseThrowable[F]): fs2.Stream[F, T] = {
    if (consistentRead) {
      fs2.Stream.raiseError(
        UnsupportedArgument("Consistent read is not supported")
      )
    } else {
      super.retrieve(query, consistentRead)
    }
  }

  @deprecated("Use retrieve without limit", "2022-04-24")
  def retrieve[T: Decoder](
    partitionKey: P,
    limit: Int
  )(implicit RT: RaiseThrowable[F]): fs2.Stream[F, T] =
    super.retrieve(Query(partitionKey), consistentRead = false, limit)

  def retrieve[T: Decoder](
    partitionKey: P
  )(implicit RT: RaiseThrowable[F]): fs2.Stream[F, T] =
    super.retrieve(Query(partitionKey), consistentRead = false)
}

/** Represent a secondary index (local and global) where the index has composite keys
  * (partition key and sort key).
  *
  * @param tableName table's name
  * @param indexName index's name
  * @param partitionKeyDef partition key definition
  * @param sortKeyDef sort key definition
  * @param jClient DynamoDB java async client
  * @tparam F effect type
  * @tparam P partition key type
  * @tparam S sort key type
  */
case class SecondaryCompositeIndex[F[_]: Async, P: Encoder, S: Encoder](
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

  /** Retrieve all items with the same partition key as a fs2 Stream. A Stream is returned instead
    * of a list because the number of returned items can be very large. This always performs eventually
    * consistent reads as strong consistent is not supported for secondary index.
    *
    * @param partitionKey partition key
    * @param limit number of items to be returned per API call
    * @param RT implicit evidence for RaiseThrowable
    * @tparam T returned item's type
    * @return a fs2 Stream of items
    */
  @deprecated("Use retrieve without limit", "2022-04-24")
  def retrieve[T: Decoder](
    partitionKey: P,
    limit: Int
  )(implicit RT: RaiseThrowable[F]): fs2.Stream[F, T] =
    retrieveOp[F, P, T](
      index,
      partitionKey,
      consistentRead = false,
      limit.some
    )(jClient)

  /** Retrieve all items with the same partition key as a fs2 Stream. A Stream is returned instead
    * of a list because the number of returned items can be very large. This always performs eventually
    * consistent reads as strong consistent is not supported for secondary index.
    *
    * @param partitionKey partition key
    * @param RT implicit evidence for RaiseThrowable
    * @tparam T returned item's type
    * @return a fs2 Stream of items
    */
  def retrieve[T: Decoder](
    partitionKey: P
  )(implicit RT: RaiseThrowable[F]): fs2.Stream[F, T] =
    retrieveOp[F, P, T](
      index,
      partitionKey,
      consistentRead = false,
      limit = None
    )(jClient)
}

/** Represent a table where the index is composite keys (partition key and sort key).
  *
  * @param tableName table's name
  * @param partitionKeyDef partition key definition
  * @param sortKeyDef sort key definition
  * @param jClient DynamoDB java async client
  * @tparam P partition key's type
  * @tparam S sort key's type
  */
case class CompositeTable[F[_]: Async, P: Encoder, S: Encoder](
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

  /** Get a single item by composite keys.
    *
    * @param partitionKey partition key
    * @param sortKey sort key
    * @param consistentRead flag to enable strongly consistent read
    * @tparam T returned item's type
    * @return an optional item of type T
    */
  def get[T: Decoder](
    partitionKey: P,
    sortKey: S,
    consistentRead: Boolean
  ): F[Option[T]] =
    getOp[F, P, S, T](table, partitionKey, sortKey, consistentRead)(jClient)

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

  /** Delete an item by composite keys.
    *
    * @param partitionKey partition key
    * @param sortKey sort key
    * @return Unit
    */
  def delete(partitionKey: P, sortKey: S): F[Unit] =
    deleteOp[F, P, S, Unit](table, partitionKey, sortKey, ReturnValue.NONE)(
      jClient
    ).void

  /** Update an item by composite keys given an update expression when a condition expression is
    * fulfilled. Return Unit.
    *
    * @param partitionKey partition key
    * @param sortKey sort key
    * @param update update expression
    * @param condition conditional expression
    * @return Unit
    */
  def update(
    partitionKey: P,
    sortKey: S,
    update: Expression,
    condition: Expression = Expression.empty
  ): F[Unit] =
    updateOp[F, P, S](table, partitionKey, sortKey, update, condition)(
      jClient
    )

  /** Update an item by composite keys given an update expression when a condition expression is
    * fulfilled. Return item is customizable via `returnValue` parameter.
    *
    * @param partitionKey partition key
    * @param sortKey sort key
    * @param returnValue flag to define which item to be returned
    * @param update update expression
    * @param condition conditional expression
    * @tparam T returned item's type
    * @return an optional item of type T
    */
  def update[T: Decoder](
    partitionKey: P,
    sortKey: S,
    returnValue: ReturnValue,
    update: Expression,
    condition: Expression
  ): F[Option[T]] =
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

  /** Retrieve all items with the same partition key as a fs2 Stream.  A Stream is returned instead
    * of a list because the number of returned items can be very large.
    *
    * @param partitionKey partition key
    * @param consistentRead flag to enable strongly consistent read
    * @param limit number of items to be returned per API call
    * @param RT implicit evidence for RaiseThrowable
    * @tparam T returned item's type
    * @return a fs2 Stream of items
    */
  @deprecated("Use retrieve without limit", "2022-04-24")
  def retrieve[T: Decoder](
    partitionKey: P,
    consistentRead: Boolean,
    limit: Int
  )(implicit RT: RaiseThrowable[F]): fs2.Stream[F, T] =
    retrieveOp[F, P, T](
      index,
      partitionKey,
      consistentRead,
      limit.some
    )(jClient)

  /** Retrieve all items with the same partition key as a fs2 Stream. A Stream is returned instead
    * of a list because the number of returned items can be very large.
    *
    * @param partitionKey partition key
    * @param consistentRead flag to enable strongly consistent read
    * @param RT implicit evidence for RaiseThrowable
    * @tparam T returned item's type
    * @return a fs2 Stream of items
    */
  def retrieve[T: Decoder](
    partitionKey: P,
    consistentRead: Boolean
  )(implicit RT: RaiseThrowable[F]): fs2.Stream[F, T] =
    retrieveOp[F, P, T](
      index,
      partitionKey,
      consistentRead,
      limit = None
    )(jClient)

  /** Get items by composite keys in batch. Max batch size is preset to 100 (maximum batch size
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
    * @return a fs2 Pipe from composite keys P and S as a tuple to T
    */
  def batchGet[T: Decoder](
    consistentRead: Boolean,
    projection: Expression,
    maxBatchWait: FiniteDuration,
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  ): Pipe[F, (P, S), T] =
    batchGetOp[F, P, S, T](
      table,
      consistentRead,
      projection,
      maxBatchWait,
      parallelism,
      backoffStrategy
    )(jClient)

  /** Put items in batch, '''in ordered'''. Meaning batches are processed in '''serial''' to avoid
    * race condition when writing items with the same composite keys. If your input doesn't have
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
    * if your input has items with the same composite keys, this can cause a race condition,
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

  /** Delete items by composite keys in batch. Max batch size is preset to 100 (maximum batch size
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
  ): Pipe[F, (P, S), Unit] =
    batchDeleteUnorderedOp[F, P, S](
      table,
      maxBatchWait,
      parallelism,
      backoffStrategy
    )(jClient)

  /** Write items (put or delete) in batch, '''in ordered'''. Meaning batches are processed in
    * '''serial''' to avoid race condition when writing items with the same composite keys. Max
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
    * @return a fs2 Pipe from Either[(P, S), T], represent deletion (Left) or put (Right) to Unit.
    */
  def batchWrite[T: Encoder](
    maxBatchWait: FiniteDuration,
    backoffStrategy: BackoffStrategy
  ): Pipe[F, Either[(P, S), T], Unit] =
    batchWriteInorderedOp[F, P, S, T](table, maxBatchWait, backoffStrategy)(
      jClient
    )
}
