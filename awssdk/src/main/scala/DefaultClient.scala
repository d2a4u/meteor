package meteor

import cats.effect.Async
import cats.implicits._
import fs2.{Pipe, RaiseThrowable, Stream}
import meteor.api._
import meteor.codec.{Decoder, Encoder}
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.collection.immutable.Iterable
import scala.concurrent.duration._

private[meteor] class DefaultClient[F[_]: Async: RaiseThrowable](
  jClient: DynamoDbAsyncClient
) extends Client[F]
    with DeleteOps
    with TableOps
    with GetOps
    with PutOps
    with ScanOps
    with UpdateOps
    with BatchWriteOps
    with BatchGetOps {

  def get[P: Encoder, U: Decoder](
    table: PartitionKeyTable[P],
    partitionKey: P,
    consistentRead: Boolean
  ): F[Option[U]] =
    getOp[F, P, U](table, partitionKey, consistentRead)(jClient)

  def get[P: Encoder, S: Encoder, U: Decoder](
    table: CompositeKeysTable[P, S],
    partitionKey: P,
    sortKey: S,
    consistentRead: Boolean
  ): F[Option[U]] =
    getOp[F, P, S, U](table, partitionKey, sortKey, consistentRead)(jClient)

  def retrieve[P: Encoder, U: Decoder](
    index: PartitionKeyIndex[P],
    query: Query[P, Nothing],
    consistentRead: Boolean
  ): F[Option[U]] =
    retrieveOp[F, P, U](index, query, consistentRead)(jClient)

  def retrieve[P: Encoder, S: Encoder, U: Decoder](
    index: CompositeKeysIndex[P, S],
    query: Query[P, S],
    consistentRead: Boolean,
    limit: Int
  ): fs2.Stream[F, U] =
    retrieveOp[F, P, S, U](index, query, consistentRead, limit)(jClient)

  def retrieve[
    P: Encoder,
    U: Decoder
  ](
    index: CompositeKeysIndex[P, _],
    partitionKey: P,
    consistentRead: Boolean,
    limit: Int
  ): fs2.Stream[F, U] =
    retrieveOp[F, P, U](index, partitionKey, consistentRead, limit)(jClient)

  def put[T: Encoder](
    tableName: String,
    t: T
  ): F[Unit] = putOp[F, T](tableName, t, Expression.empty)(jClient)

  def put[T: Encoder](
    tableName: String,
    t: T,
    condition: Expression
  ): F[Unit] = putOp[F, T](tableName, t, condition)(jClient)

  def put[T: Encoder, U: Decoder](
    tableName: String,
    t: T
  ): F[Option[U]] = putOp[F, T, U](tableName, t, Expression.empty)(jClient)

  def put[T: Encoder, U: Decoder](
    tableName: String,
    t: T,
    condition: Expression
  ): F[Option[U]] = putOp[F, T, U](tableName, t, condition)(jClient)

  def delete[P: Encoder, S: Encoder](
    table: CompositeKeysTable[P, S],
    partitionKey: P,
    sortKey: S,
    condition: Expression
  ): F[Unit] =
    deleteOp[F, P, S, Unit](table, partitionKey, sortKey, condition, ReturnValue.NONE)(
      jClient
    ).void

  def delete[P: Encoder](
    table: PartitionKeyTable[P],
    partitionKey: P,
    condition: Expression
  ): F[Unit] =
    deleteOp[F, P, Unit](table, partitionKey, condition, ReturnValue.NONE)(jClient).void

  def scan[T: Decoder](
    tableName: String,
    filter: Expression,
    consistentRead: Boolean,
    parallelism: Int
  ): fs2.Stream[F, T] =
    scanOp[F, T](tableName, filter, consistentRead, parallelism)(jClient)

  def scan[T: Decoder](
    tableName: String,
    consistentRead: Boolean,
    parallelism: Int
  ): fs2.Stream[F, T] =
    scanOp[F, T](tableName, consistentRead, parallelism)(jClient)

  def update[P: Encoder, U: Decoder](
    table: PartitionKeyTable[P],
    partitionKey: P,
    update: Expression,
    returnValue: ReturnValue
  ): F[Option[U]] =
    updateOp[F, P, U](table, partitionKey, update, returnValue)(jClient)

  def update[P: Encoder, U: Decoder](
    table: PartitionKeyTable[P],
    partitionKey: P,
    update: Expression,
    condition: Expression,
    returnValue: ReturnValue
  ): F[Option[U]] =
    updateOp[F, P, U](table, partitionKey, update, condition, returnValue)(
      jClient
    )

  def update[P: Encoder, S: Encoder, U: Decoder](
    table: CompositeKeysTable[P, S],
    partitionKey: P,
    sortKey: S,
    update: Expression,
    returnValue: ReturnValue
  ): F[Option[U]] =
    updateOp[F, P, S, U](table, partitionKey, sortKey, update, returnValue)(
      jClient
    )

  def update[P: Encoder, S: Encoder, U: Decoder](
    table: CompositeKeysTable[P, S],
    partitionKey: P,
    sortKey: S,
    update: Expression,
    condition: Expression,
    returnValue: ReturnValue
  ): F[Option[U]] =
    updateOp[F, P, S, U](
      table,
      partitionKey,
      sortKey,
      update,
      condition,
      returnValue
    )(
      jClient
    )

  def update[P: Encoder](
    table: PartitionKeyTable[P],
    partitionKey: P,
    update: Expression
  ): F[Unit] =
    updateOp[F, P](table, partitionKey, update)(jClient)

  def update[P: Encoder](
    table: PartitionKeyTable[P],
    partitionKey: P,
    update: Expression,
    condition: Expression
  ): F[Unit] =
    updateOp[F, P](table, partitionKey, update, condition)(jClient)

  def update[P: Encoder, S: Encoder](
    table: CompositeKeysTable[P, S],
    partitionKey: P,
    sortKey: S,
    update: Expression
  ): F[Unit] =
    updateOp[F, P, S](table, partitionKey, sortKey, update)(
      jClient
    )

  def update[P: Encoder, S: Encoder](
    table: CompositeKeysTable[P, S],
    partitionKey: P,
    sortKey: S,
    update: Expression,
    condition: Expression
  ): F[Unit] =
    updateOp[F, P, S](table, partitionKey, sortKey, update, condition)(
      jClient
    )

  def batchGet(
    requests: Map[String, BatchGet],
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  ): F[Map[String, Iterable[AttributeValue]]] =
    batchGetOp[F](requests, parallelism, backoffStrategy)(jClient)

  def batchGet[P: Encoder, U: Decoder](
    table: PartitionKeyTable[P],
    consistentRead: Boolean,
    projection: Expression,
    keys: Iterable[P],
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  ): F[Iterable[U]] =
    batchGetOp[F, P, U](
      table,
      consistentRead,
      projection,
      keys,
      parallelism,
      backoffStrategy
    )(jClient)

  def batchGet[P: Encoder, S: Encoder, U: Decoder](
    table: CompositeKeysTable[P, S],
    consistentRead: Boolean,
    projection: Expression,
    keys: Iterable[(P, S)],
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  ): F[Iterable[U]] =
    batchGetOp[F, P, S, U](
      table,
      consistentRead,
      projection,
      keys,
      parallelism,
      backoffStrategy
    )(jClient)

  def batchGet[P: Encoder, U: Decoder](
    table: PartitionKeyTable[P],
    consistentRead: Boolean,
    projection: Expression,
    maxBatchWait: FiniteDuration,
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  ): Pipe[F, P, U] =
    batchGetOp[F, P, U](
      table,
      consistentRead,
      projection,
      maxBatchWait,
      parallelism,
      backoffStrategy
    )(jClient)

  def batchGet[P: Encoder, S: Encoder, U: Decoder](
    table: CompositeKeysTable[P, S],
    consistentRead: Boolean,
    projection: Expression,
    maxBatchWait: FiniteDuration,
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  ): Pipe[F, (P, S), U] =
    batchGetOp[F, P, S, U](
      table,
      consistentRead,
      projection,
      maxBatchWait,
      parallelism,
      backoffStrategy
    )(jClient)

  def batchGet[P: Encoder, U: Decoder](
    table: PartitionKeyTable[P],
    consistentRead: Boolean,
    keys: Iterable[P],
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  ): F[Iterable[U]] =
    batchGetOp[F, P, U](
      table,
      consistentRead,
      Expression.empty,
      keys,
      parallelism,
      backoffStrategy
    )(
      jClient
    )

  def batchGet[P: Encoder, S: Encoder, U: Decoder](
    table: CompositeKeysTable[P, S],
    consistentRead: Boolean,
    keys: Iterable[(P, S)],
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  ): F[Iterable[U]] =
    batchGetOp[F, P, S, U](
      table,
      consistentRead,
      Expression.empty,
      keys,
      parallelism,
      backoffStrategy
    )(
      jClient
    )

  def batchWrite[DP: Encoder, P: Encoder](
    table: PartitionKeyTable[DP],
    maxBatchWait: FiniteDuration,
    backoffStrategy: BackoffStrategy
  ): Pipe[F, Either[DP, P], Unit] =
    batchWriteInorderedOp[F, DP, P](table, maxBatchWait, backoffStrategy)(
      jClient
    )

  def batchWrite[DP: Encoder, DS: Encoder, P: Encoder](
    table: CompositeKeysTable[DP, DS],
    maxBatchWait: FiniteDuration,
    backoffStrategy: BackoffStrategy
  ): Pipe[F, Either[(DP, DS), P], Unit] =
    batchWriteInorderedOp[F, DP, DS, P](table, maxBatchWait, backoffStrategy)(
      jClient
    )

  def batchPut[T: Encoder](
    table: Index[_],
    maxBatchWait: FiniteDuration,
    backoffStrategy: BackoffStrategy
  ): Pipe[F, T, Unit] =
    batchPutInorderedOp[F, T](table, maxBatchWait, backoffStrategy)(jClient)

  def batchPut[T: Encoder](
    table: Index[_],
    items: Iterable[T],
    backoffStrategy: BackoffStrategy
  ): F[Unit] = {
    val itemsStream = Stream.iterable(items).covary[F]
    val pipe =
      batchPutInorderedOp[F, T](table, Int.MaxValue.seconds, backoffStrategy)(
        jClient
      )
    pipe.apply(itemsStream).compile.drain
  }

  def batchPutUnordered[T: Encoder](
    table: Index[_],
    items: Set[T],
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  ): F[Unit] = {
    val itemsStream = Stream.iterable(items).covary[F]
    val pipe =
      batchPutUnorderedOp[F, T](
        table.tableName,
        Int.MaxValue.seconds,
        parallelism,
        backoffStrategy
      )(jClient)
    pipe.apply(itemsStream).compile.drain
  }

  def batchDelete[P: Encoder](
    table: PartitionKeyTable[P],
    maxBatchWait: FiniteDuration,
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  ): Pipe[F, P, Unit] =
    _.through(
      batchDeleteUnorderedOp[F, P](
        table,
        maxBatchWait,
        parallelism,
        backoffStrategy
      )(
        jClient
      )
    )

  def batchDelete[P: Encoder, S: Encoder](
    table: CompositeKeysTable[P, S],
    maxBatchWait: FiniteDuration,
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  ): Pipe[F, (P, S), Unit] =
    _.through(
      batchDeleteUnorderedOp[F, P, S](
        table,
        maxBatchWait,
        parallelism,
        backoffStrategy
      )(
        jClient
      )
    )

  def describe(tableName: String): F[TableDescription] =
    describeOp[F](tableName)(jClient)

  def createCompositeKeysTable[P, S](
    tableName: String,
    partitionKeyDef: KeyDef[P],
    sortKeyDef: KeyDef[S],
    billingMode: BillingMode,
    attributeDefinition: Map[String, DynamoDbType] = Map.empty,
    globalSecondaryIndexes: Set[GlobalSecondaryIndex] = Set.empty,
    localSecondaryIndexes: Set[LocalSecondaryIndex] = Set.empty
  ): F[Unit] =
    createCompositeKeysTableOp[F, P, S](
      tableName,
      partitionKeyDef,
      sortKeyDef,
      attributeDefinition,
      globalSecondaryIndexes,
      localSecondaryIndexes,
      billingMode,
      waitTillReady = true
    )(jClient)

  def createPartitionKeyTable[P](
    tableName: String,
    partitionKeyDef: KeyDef[P],
    billingMode: BillingMode,
    attributeDefinition: Map[String, DynamoDbType] = Map.empty,
    globalSecondaryIndexes: Set[GlobalSecondaryIndex] = Set.empty,
    localSecondaryIndexes: Set[LocalSecondaryIndex] = Set.empty
  ): F[Unit] =
    createPartitionKeyTableOp[F, P](
      tableName,
      partitionKeyDef,
      attributeDefinition,
      globalSecondaryIndexes,
      localSecondaryIndexes,
      billingMode,
      waitTillReady = true
    )(jClient)

  def deleteTable(tableName: String): F[Unit] =
    deleteTableOp[F](tableName)(jClient)
}
