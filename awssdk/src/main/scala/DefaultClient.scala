package meteor

import cats.effect.Concurrent
import fs2.RaiseThrowable
import meteor.api._
import meteor.codec.{Decoder, Encoder}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

class DefaultClient[F[_]: Concurrent: RaiseThrowable](
  jClient: DynamoDbAsyncClient
) extends Client[F]
    with DeleteOps
    with DescribeOps
    with GetOps
    with PutOps
    with ScanOps
    with UpdateOps {

  def get[U: Decoder, P: Encoder](
    table: Table,
    partitionKey: P,
    consistentRead: Boolean
  ): F[Option[U]] = getOp[F, U, P](table, partitionKey, consistentRead)(jClient)

  def get[U: Decoder, P: Encoder, S: Encoder](
    table: Table,
    partitionKey: P,
    sortKey: S,
    consistentRead: Boolean
  ): F[Option[U]] =
    getOp[F, U, P, S](table, partitionKey, sortKey, consistentRead)(jClient)

  def retrieve[T: Decoder, P: Encoder, S: Encoder](
    table: Table,
    query: Query[P, S],
    consistentRead: Boolean,
    limit: Int
  ): fs2.Stream[F, T] =
    retrieveOp[F, T, P, S](table, query, consistentRead, limit)(jClient)

  def retrieve[T: Decoder, P: Encoder, S: Encoder](
    table: Table,
    query: Query[P, S],
    consistentRead: Boolean,
    index: Index,
    limit: Int
  ): fs2.Stream[F, T] =
    retrieveOp[F, T, P, S](table, query, consistentRead, index, limit)(jClient)

  def put[T: Encoder](
    table: Table,
    t: T
  ): F[Unit] = putOp[F, T](table, t)(jClient)

  def put[T: Encoder](
    table: Table,
    t: T,
    condition: Expression
  ): F[Unit] = putOp[F, T](table, t, condition)(jClient)

  def put[T: Encoder, U: Decoder](
    table: Table,
    t: T
  ): F[Option[U]] = putOp[F, T, U](table, t)(jClient)

  def put[T: Encoder, U: Decoder](
    table: Table,
    t: T,
    condition: Expression
  ): F[Option[U]] = putOp[F, T, U](table, t, condition)(jClient)

  def delete[P: Encoder, S: Encoder](
    table: Table,
    partitionKey: P,
    sortKey: S
  ): F[Unit] = deleteOp[F, P, S](table, partitionKey, sortKey)(jClient)

  def delete[P: Encoder](
    table: Table,
    partitionKey: P
  ): F[Unit] = deleteOp[F, P](table, partitionKey)(jClient)

  def scan[T: Decoder](
    table: Table,
    filter: Expression,
    consistentRead: Boolean,
    parallelism: Int
  ): fs2.Stream[F, Option[T]] =
    scanOp[F, T](table, filter, consistentRead, parallelism)(jClient)

  def scan[T: Decoder](
    table: Table,
    consistentRead: Boolean,
    parallelism: Int
  ): fs2.Stream[F, Option[T]] =
    scanOp[F, T](table, consistentRead, parallelism)(jClient)

  def update[P: Encoder, U: Decoder](
    table: Table,
    partitionKey: P,
    update: Expression,
    returnValue: ReturnValue
  ): F[Option[U]] =
    updateOp[F, P, U](table, partitionKey, update, returnValue)(jClient)

  def update[P: Encoder, U: Decoder](
    table: Table,
    partitionKey: P,
    update: Expression,
    condition: Expression,
    returnValue: ReturnValue
  ): F[Option[U]] =
    updateOp[F, P, U](table, partitionKey, update, condition, returnValue)(
      jClient
    )

  def update[P: Encoder, S: Encoder, U: Decoder](
    table: Table,
    partitionKey: P,
    sortKey: S,
    update: Expression,
    returnValue: ReturnValue
  ): F[Option[U]] =
    updateOp[F, P, S, U](table, partitionKey, sortKey, update, returnValue)(
      jClient
    )

  def update[P: Encoder, S: Encoder, U: Decoder](
    table: Table,
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
    table: Table,
    partitionKey: P,
    update: Expression
  ): F[Unit] =
    updateOp[F, P](table, partitionKey, update)(jClient)

  def update[P: Encoder](
    table: Table,
    partitionKey: P,
    update: Expression,
    condition: Expression
  ): F[Unit] =
    updateOp[F, P](table, partitionKey, update, condition)(jClient)

  def update[P: Encoder, S: Encoder](
    table: Table,
    partitionKey: P,
    sortKey: S,
    update: Expression
  ): F[Unit] =
    updateOp[F, P, S](table, partitionKey, sortKey, update)(
      jClient
    )

  def update[P: Encoder, S: Encoder](
    table: Table,
    partitionKey: P,
    sortKey: S,
    update: Expression,
    condition: Expression
  ): F[Unit] =
    updateOp[F, P, S](table, partitionKey, sortKey, update, condition)(
      jClient
    )

  def describe(table: Table): F[TableDescription] =
    describeOp[F](table)(jClient)
}
