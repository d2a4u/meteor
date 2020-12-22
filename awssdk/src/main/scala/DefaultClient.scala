package meteor

import cats.effect.{Concurrent, Timer}
import fs2.{Pipe, RaiseThrowable, Stream}
import meteor.api._
import meteor.codec.{Decoder, Encoder}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.collection.immutable.Iterable
import scala.concurrent.duration._

class DefaultClient[F[_]: Concurrent: Timer: RaiseThrowable](
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

  def get[U: Decoder, P: Encoder](
    tableName: String,
    partitionKey: P,
    consistentRead: Boolean
  ): F[Option[U]] =
    getOp[F, U, P](tableName, partitionKey, consistentRead)(jClient)

  def get[U: Decoder, P: Encoder, S: Encoder](
    tableName: String,
    partitionKey: P,
    sortKey: S,
    consistentRead: Boolean
  ): F[Option[U]] =
    getOp[F, U, P, S](tableName, partitionKey, sortKey, consistentRead)(jClient)

  def retrieve[U: Decoder, P: Encoder, S: Encoder](
    tableName: String,
    query: Query[P, S],
    consistentRead: Boolean,
    limit: Int
  ): fs2.Stream[F, U] =
    retrieveOp[F, U, P, S](tableName, query, consistentRead, limit)(jClient)

  def retrieve[U: Decoder, P: Encoder, S: Encoder](
    tableName: String,
    query: Query[P, S],
    consistentRead: Boolean,
    index: Index,
    limit: Int
  ): fs2.Stream[F, U] =
    retrieveOp[F, U, P, S](tableName, query, consistentRead, index, limit)(
      jClient
    )

  def retrieve[
    U: Decoder,
    P: Encoder
  ](
    tableName: String,
    partitionKey: P,
    consistentRead: Boolean,
    limit: Int
  ): fs2.Stream[F, U] =
    retrieveOp[F, U, P](tableName, partitionKey, consistentRead, limit)(jClient)

  def retrieve[
    U: Decoder,
    P: Encoder
  ](
    tableName: String,
    partitionKey: P,
    consistentRead: Boolean,
    index: Index,
    limit: Int
  ): fs2.Stream[F, U] =
    retrieveOp[F, U, P](tableName, partitionKey, consistentRead, index, limit)(
      jClient
    )

  def put[T: Encoder](
    tableName: String,
    t: T
  ): F[Unit] = putOp[F, T](tableName, t)(jClient)

  def put[T: Encoder](
    tableName: String,
    t: T,
    condition: Expression
  ): F[Unit] = putOp[F, T](tableName, t, condition)(jClient)

  def put[T: Encoder, U: Decoder](
    tableName: String,
    t: T
  ): F[Option[U]] = putOp[F, T, U](tableName, t)(jClient)

  def put[T: Encoder, U: Decoder](
    tableName: String,
    t: T,
    condition: Expression
  ): F[Option[U]] = putOp[F, T, U](tableName, t, condition)(jClient)

  def delete[P: Encoder, S: Encoder](
    tableName: String,
    partitionKey: P,
    sortKey: S
  ): F[Unit] = deleteOp[F, P, S](tableName, partitionKey, sortKey)(jClient)

  def delete[P: Encoder](
    tableName: String,
    partitionKey: P
  ): F[Unit] = deleteOp[F, P](tableName, partitionKey)(jClient)

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
    tableName: String,
    partitionKey: P,
    update: Expression,
    returnValue: ReturnValue
  ): F[Option[U]] =
    updateOp[F, P, U](tableName, partitionKey, update, returnValue)(jClient)

  def update[P: Encoder, U: Decoder](
    tableName: String,
    partitionKey: P,
    update: Expression,
    condition: Expression,
    returnValue: ReturnValue
  ): F[Option[U]] =
    updateOp[F, P, U](tableName, partitionKey, update, condition, returnValue)(
      jClient
    )

  def update[P: Encoder, S: Encoder, U: Decoder](
    tableName: String,
    partitionKey: P,
    sortKey: S,
    update: Expression,
    returnValue: ReturnValue
  ): F[Option[U]] =
    updateOp[F, P, S, U](tableName, partitionKey, sortKey, update, returnValue)(
      jClient
    )

  def update[P: Encoder, S: Encoder, U: Decoder](
    tableName: String,
    partitionKey: P,
    sortKey: S,
    update: Expression,
    condition: Expression,
    returnValue: ReturnValue
  ): F[Option[U]] =
    updateOp[F, P, S, U](
      tableName,
      partitionKey,
      sortKey,
      update,
      condition,
      returnValue
    )(
      jClient
    )

  def update[P: Encoder](
    tableName: String,
    partitionKey: P,
    update: Expression
  ): F[Unit] =
    updateOp[F, P](tableName, partitionKey, update)(jClient)

  def update[P: Encoder](
    tableName: String,
    partitionKey: P,
    update: Expression,
    condition: Expression
  ): F[Unit] =
    updateOp[F, P](tableName, partitionKey, update, condition)(jClient)

  def update[P: Encoder, S: Encoder](
    tableName: String,
    partitionKey: P,
    sortKey: S,
    update: Expression
  ): F[Unit] =
    updateOp[F, P, S](tableName, partitionKey, sortKey, update)(
      jClient
    )

  def update[P: Encoder, S: Encoder](
    tableName: String,
    partitionKey: P,
    sortKey: S,
    update: Expression,
    condition: Expression
  ): F[Unit] =
    updateOp[F, P, S](tableName, partitionKey, sortKey, update, condition)(
      jClient
    )

  def batchGet(
    requests: Map[String, BatchGet]
  ): F[Map[String, Iterable[AttributeValue]]] =
    batchGetOp[F](requests)(jClient)

  def batchGet[T: Encoder, U: Decoder](
    tableName: String,
    consistentRead: Boolean,
    projection: Expression,
    keys: Iterable[T]
  ): F[Iterable[U]] =
    batchGetOp[F, T, U](
      tableName,
      consistentRead,
      projection,
      keys
    )(jClient)

  def batchGet[T: Encoder, U: Decoder](
    tableName: String,
    consistentRead: Boolean,
    projection: Expression,
    maxBatchWait: FiniteDuration,
    parallelism: Int
  ): Pipe[F, T, U] =
    batchGetOp[F, T, U](
      tableName,
      consistentRead,
      projection,
      maxBatchWait,
      parallelism
    )(jClient)

  def batchGet[T: Encoder, U: Decoder](
    tableName: String,
    consistentRead: Boolean,
    keys: Iterable[T]
  ): F[Iterable[U]] =
    batchGetOp[F, T, U](tableName, consistentRead, Expression.empty, keys)(
      jClient
    )

  def batchWrite[D: Encoder, P: Encoder](
    table: Table,
    maxBatchWait: FiniteDuration
  ): Pipe[F, Either[D, P], Unit] =
    batchWriteInorderedOp[F, D, P](table, maxBatchWait)(jClient)

  def batchPut[T: Encoder](
    table: Table,
    maxBatchWait: FiniteDuration
  ): Pipe[F, T, Unit] =
    batchPutInorderedOp[F, T](table, maxBatchWait)(jClient)

  def batchPut[T: Encoder](
    table: Table,
    items: Iterable[T]
  ): F[Unit] = {
    val itemsStream = Stream.iterable(items).covary[F]
    val pipe =
      batchPutInorderedOp[F, T](table, Int.MaxValue.seconds)(jClient)
    pipe.apply(itemsStream).compile.drain
  }

  def batchPutUnordered[T: Encoder](
    tableName: String,
    items: Set[T],
    maxBatchWait: FiniteDuration,
    parallelism: Int
  ): F[Unit] = {
    val itemsStream = Stream.iterable(items).covary[F]
    val pipe =
      batchPutUnorderedOp[F, T](tableName, maxBatchWait, parallelism)(jClient)
    pipe.apply(itemsStream).compile.drain
  }

  def batchDelete[P: Encoder](
    tableName: String,
    maxBatchWait: FiniteDuration,
    parallelism: Int
  ): Pipe[F, P, Unit] =
    _.through(
      batchDeleteUnorderedOp[F, P](tableName, maxBatchWait, parallelism)(
        jClient
      )
    )

  def batchDelete[P: Encoder, S: Encoder](
    tableName: String,
    maxBatchWait: FiniteDuration,
    parallelism: Int
  ): Pipe[F, (P, S), Unit] =
    _.through(
      batchDeleteUnorderedOp[F, (P, S)](tableName, maxBatchWait, parallelism)(
        jClient
      )
    )

  def describe(tableName: String): F[TableDescription] =
    describeOp[F](tableName)(jClient)

  def createTable(
    table: Table,
    billingMode: BillingMode
  ): F[Unit] =
    createTableOp[F](table, billingMode, waitTillReady = true)(jClient)

  def deleteTable(tableName: String): F[Unit] =
    deleteTableOp[F](tableName)(jClient)
}
