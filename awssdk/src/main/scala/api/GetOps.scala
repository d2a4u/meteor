package meteor
package api

import cats.effect.Concurrent
import cats.implicits._
import fs2.RaiseThrowable
import meteor.codec.{Decoder, Encoder}
import meteor.errors._
import meteor.implicits._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.jdk.CollectionConverters._

trait GetOps {
  def getOp[F[_]: Concurrent, P: Encoder, U: Decoder](
    table: Table,
    partitionKey: P,
    consistentRead: Boolean
  )(jClient: DynamoDbAsyncClient): F[Option[U]] = {
    val query = table.keys(partitionKey, None)
    val req =
      GetItemRequest.builder()
        .consistentRead(consistentRead)
        .tableName(table.name)
        .key(query)
        .build()
    (() => jClient.getItem(req)).liftF[F].flatMap { resp =>
      if (resp.hasItem()) {
        Concurrent[F].fromEither(
          resp.item().asAttributeValue.as[U].map(_.some).leftWiden[Throwable]
        )
      } else {
        none[U].pure[F]
      }
    }
  }

  def getOp[F[_]: Concurrent, P: Encoder, S: Encoder, U: Decoder](
    table: Table,
    partitionKey: P,
    sortKey: S,
    consistentRead: Boolean
  )(jClient: DynamoDbAsyncClient): F[Option[U]] = {
    val query = table.keys(partitionKey, sortKey.some)
    val req =
      GetItemRequest.builder()
        .consistentRead(consistentRead)
        .tableName(table.name)
        .key(query)
        .build()
    (() => jClient.getItem(req)).liftF[F].flatMap { resp =>
      if (resp.hasItem()) {
        Concurrent[F].fromEither(
          resp.item().asAttributeValue.as[U].map(_.some).leftWiden[Throwable]
        )
      } else {
        none[U].pure[F]
      }
    }
  }

  def retrieveOp[
    F[_]: Concurrent: RaiseThrowable,
    P: Encoder,
    T: Decoder
  ](
    table: Table,
    partitionKey: P,
    consistentRead: Boolean,
    limit: Int
  )(jClient: DynamoDbAsyncClient): fs2.Stream[F, T] = {
    val query = Query[P](partitionKey)
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
    P: Encoder,
    T: Decoder
  ](
    table: Table,
    partitionKey: P,
    consistentRead: Boolean,
    index: Index,
    limit: Int
  )(jClient: DynamoDbAsyncClient): fs2.Stream[F, T] = {
    val query = Query[P](partitionKey)
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
    P: Encoder,
    S: Encoder,
    U: Decoder
  ](
    table: Table,
    query: Query[P, S],
    consistentRead: Boolean,
    limit: Int
  )(jClient: DynamoDbAsyncClient): fs2.Stream[F, U] =
    mkBuilder[F, P, S](
      table,
      query,
      consistentRead,
      None,
      limit
    ).flatMap(builder => sendQueryRequest[F, U](builder)(jClient))

  def retrieveOp[
    F[_]: Concurrent: RaiseThrowable,
    P: Encoder,
    S: Encoder,
    U: Decoder
  ](
    table: Table,
    query: Query[P, S],
    consistentRead: Boolean,
    index: Index,
    limit: Int
  )(jClient: DynamoDbAsyncClient): fs2.Stream[F, U] =
    mkBuilder[F, P, S](
      table,
      query,
      consistentRead,
      Some(index),
      limit
    ).flatMap(builder => sendQueryRequest[F, U](builder)(jClient))

  private def sendQueryRequest[F[_]: Concurrent: RaiseThrowable, U: Decoder](
    builder: QueryRequest.Builder
  )(jClient: DynamoDbAsyncClient): fs2.Stream[F, U] = {
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

    type FailureOr[T] = Either[DecoderError, T]

    for {
      resp <- doQuery(builder.build())
      listT <- fs2.Stream.fromEither(
        resp.items().asScala.toList.traverse[FailureOr, U](
          _.asAttributeValue.as[U]
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
    val exp = query.keyCondition(table)
    fs2.Stream.fromEither[F](
      if (exp.nonEmpty) Right(exp)
      else Left(InvalidExpression)
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
