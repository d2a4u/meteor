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
      limit
    ).flatMap(builder => sendQueryRequest[F, T](builder)(jClient))
  }

  def retrieveOp[
    F[_]: Concurrent: RaiseThrowable,
    P: Encoder,
    T: Decoder
  ](
    secondaryIndex: SecondaryIndex,
    partitionKey: P,
    consistentRead: Boolean,
    limit: Int
  )(jClient: DynamoDbAsyncClient): fs2.Stream[F, T] = {
    val query = Query[P](partitionKey)
    mkBuilder[F, P, Nothing](
      secondaryIndex,
      query,
      consistentRead,
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
      limit
    ).flatMap(builder => sendQueryRequest[F, U](builder)(jClient))

  def retrieveOp[
    F[_]: Concurrent: RaiseThrowable,
    P: Encoder,
    S: Encoder,
    U: Decoder
  ](
    secondaryIndex: SecondaryIndex,
    query: Query[P, S],
    consistentRead: Boolean,
    limit: Int
  )(jClient: DynamoDbAsyncClient): fs2.Stream[F, U] =
    mkBuilder[F, P, S](
      secondaryIndex,
      query,
      consistentRead,
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
    index: Index,
    query: Query[P, S],
    consistentRead: Boolean,
    limit: Int
  ) = {
    val (tableName, optIndexName) = index match {
      case Table(name, _, _) => (name, None)
      case SecondaryIndex(tableName, indexName, _, _) =>
        (tableName, Some(indexName))
    }
    val exp = query.keyCondition(index)
    fs2.Stream.fromEither[F](
      if (exp.nonEmpty) Right(exp)
      else Left(InvalidExpression)
    ).map {
      cond =>
        val builder0 =
          QueryRequest.builder()
            .tableName(tableName)
            .consistentRead(consistentRead)
            .keyConditionExpression(cond.expression)
            .limit(limit)
        val builder1 = optIndexName.fold(builder0)(builder0.indexName)
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
