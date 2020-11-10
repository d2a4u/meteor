package meteor
package api

import cats.effect.Concurrent
import cats.implicits._
import fs2.RaiseThrowable
import meteor.codec.{Decoder, DecoderFailure, Encoder}
import meteor.errors.InvalidExpression
import meteor.implicits._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.jdk.CollectionConverters._

trait GetOps {
  def getOp[F[_]: Concurrent, U: Decoder, P: Encoder](
    table: Table,
    partitionKey: P,
    consistentRead: Boolean
  )(jClient: DynamoDbAsyncClient): F[Option[U]] = {
    val query = Encoder[P].write(partitionKey).m()
    val req =
      GetItemRequest.builder()
        .consistentRead(consistentRead)
        .tableName(table.name)
        .key(query)
        .build()
    (() => jClient.getItem(req)).liftF[F].flatMap { resp =>
      Concurrent[F].fromEither(resp.item().attemptDecode[U])
    }
  }

  def getOp[F[_]: Concurrent, U: Decoder, P: Encoder, S: Encoder](
    table: Table,
    partitionKey: P,
    sortKey: S,
    consistentRead: Boolean
  )(jClient: DynamoDbAsyncClient): F[Option[U]] = {
    val query = Encoder[(P, S)].write((partitionKey, sortKey)).m()
    val req =
      GetItemRequest.builder()
        .consistentRead(consistentRead)
        .tableName(table.name)
        .key(query)
        .build()
    (() => jClient.getItem(req)).liftF[F].flatMap { resp =>
      Concurrent[F].fromEither(resp.item().attemptDecode[U])
    }
  }

  def retrieveOp[
    F[_]: Concurrent: RaiseThrowable,
    T: Decoder,
    P: Encoder
  ](
    table: Table,
    partitionKey: P,
    consistentRead: Boolean,
    limit: Int
  )(jClient: DynamoDbAsyncClient): fs2.Stream[F, T] = {
    val query = Query[P, Nothing](partitionKey)
    mkBuilder[F, P, Nothing](
      table,
      query,
      consistentRead,
      None,
      limit
    ).flatMap(builder => sendQueryRequest[F, T](builder)(jClient))
  }

  def retrieveOp[
    F[_]: Concurrent: RaiseThrowable,
    T: Decoder,
    P: Encoder
  ](
    table: Table,
    partitionKey: P,
    consistentRead: Boolean,
    index: Index,
    limit: Int
  )(jClient: DynamoDbAsyncClient): fs2.Stream[F, T] = {
    val query = Query[P, Nothing](partitionKey)
    mkBuilder[F, P, Nothing](
      table,
      query,
      consistentRead,
      Some(index),
      limit
    ).flatMap(builder => sendQueryRequest[F, T](builder)(jClient))
  }

  def retrieveOp[
    F[_]: Concurrent: RaiseThrowable,
    T: Decoder,
    P: Encoder,
    S: Encoder
  ](
    table: Table,
    query: Query[P, S],
    consistentRead: Boolean,
    limit: Int
  )(jClient: DynamoDbAsyncClient): fs2.Stream[F, T] =
    mkBuilder[F, P, S](
      table,
      query,
      consistentRead,
      None,
      limit
    ).flatMap(builder => sendQueryRequest[F, T](builder)(jClient))

  def retrieveOp[
    F[_]: Concurrent: RaiseThrowable,
    T: Decoder,
    P: Encoder,
    S: Encoder
  ](
    table: Table,
    query: Query[P, S],
    consistentRead: Boolean,
    index: Index,
    limit: Int
  )(jClient: DynamoDbAsyncClient): fs2.Stream[F, T] =
    mkBuilder[F, P, S](
      table,
      query,
      consistentRead,
      Some(index),
      limit
    ).flatMap(builder => sendQueryRequest[F, T](builder)(jClient))

  private def sendQueryRequest[F[_]: Concurrent: RaiseThrowable, T: Decoder](
    builder: QueryRequest.Builder
  )(jClient: DynamoDbAsyncClient): fs2.Stream[F, T] = {
    def doQuery(
      req: QueryRequest
    ): fs2.Stream[F, QueryResponse] =
      fs2.Stream.eval((() => jClient.query(req)).liftF[F]).flatMap { resp =>
        if (resp.hasLastEvaluatedKey) {
          val nextReq =
            builder.exclusiveStartKey(resp.lastEvaluatedKey()).build()
          fs2.Stream.emit[F, QueryResponse](resp) ++ doQuery(nextReq)
        } else {
          fs2.Stream.emit[F, QueryResponse](resp)
        }
      }

    type FailureOr[U] = Either[DecoderFailure, U]

    for {
      resp <- doQuery(builder.build())
      listT <- fs2.Stream.fromEither(
        resp.items().asScala.toList.traverse[FailureOr, Option[T]](
          _.attemptDecode[T]
        ).map(
          _.flatten
        )
      )
      result <- fs2.Stream.emits(listT)
    } yield result
  }

  private def mkBuilder[F[_]: Concurrent, P: Encoder, S: Encoder](
    table: Table,
    query: Query[P, S],
    consistentRead: Boolean,
    index: Option[Index],
    limit: Int
  ) = {
    fs2.Stream.fromEither[F](
      Either.fromOption(
        query.keyCondition.nonEmpty.guard[Option].as(query.keyCondition),
        InvalidExpression
      )
    ).map {
      cond =>
        val builder0 =
          QueryRequest.builder()
            .tableName(table.name)
            .consistentRead(consistentRead)
            .keyConditionExpression(cond.expression)
            .limit(limit)
        val builder1 = index.fold(builder0)(i => builder0.indexName(i.name))
        if (query.filter.isEmpty) {
          builder1
            .expressionAttributeNames(cond.attributeNames.asJava)
            .expressionAttributeValues(cond.attributeValues.asJava)
        } else {
          builder1.filterExpression(
            query.filter.expression
          ).expressionAttributeNames(
            (cond.attributeNames ++ query.filter.attributeNames).asJava
          ).expressionAttributeValues(
            (cond.attributeValues ++ query.filter.attributeValues).asJava
          )
        }
    }
  }
}
