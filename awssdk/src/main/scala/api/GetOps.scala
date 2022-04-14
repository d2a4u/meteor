package meteor
package api

import cats.effect.Async
import cats.implicits._
import cats.ApplicativeThrow
import fs2.RaiseThrowable
import meteor.codec.{Decoder, Encoder}
import meteor.errors._
import meteor.implicits._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.jdk.CollectionConverters._

private[meteor] trait GetOps
    extends PartitionKeyGetOps
    with CompositeKeysGetOps {}

private[meteor] trait PartitionKeyGetOps extends SharedGetOps {
  private[meteor] def getOp[F[_]: Async, P: Encoder, U: Decoder](
    table: PartitionKeyTable[P],
    partitionKey: P,
    consistentRead: Boolean
  )(jClient: DynamoDbAsyncClient): F[Option[U]] = {
    table.mkKey[F](partitionKey).flatMap { key =>
      sendGetRequest(table.tableName, key, consistentRead)(jClient)
    }
  }

  // the different between this and getOp is that filter expression is done on server side
  private[meteor] def retrieveOp[
    F[_]: Async: RaiseThrowable,
    P: Encoder,
    T: Decoder
  ](
    index: PartitionKeyIndex[P],
    query: Query[P, Nothing],
    consistentRead: Boolean
  )(jClient: DynamoDbAsyncClient): F[Option[T]] = {
    mkQueryRequestBuilder[F, P](
      index,
      keyExpression = query.keyCondition(index),
      filterExpression = query.filter,
      consistentRead = consistentRead,
      limit = 1
    ).flatMap(builder => sendQueryRequest[F, T](builder)(jClient)).compile.last
  }
}

private[meteor] trait CompositeKeysGetOps extends SharedGetOps {
  private[meteor] def getOp[F[_]: Async, P: Encoder, S: Encoder, U: Decoder](
    table: CompositeKeysTable[P, S],
    partitionKey: P,
    sortKey: S,
    consistentRead: Boolean
  )(jClient: DynamoDbAsyncClient): F[Option[U]] = {
    table.mkKey[F](partitionKey, sortKey).flatMap { key =>
      sendGetRequest(table.tableName, key, consistentRead)(jClient)
    }
  }

  private[meteor] def retrieveOp[
    F[_]: Async: RaiseThrowable,
    P: Encoder,
    T: Decoder
  ](
    index: CompositeKeysIndex[P, _],
    partitionKey: P,
    consistentRead: Boolean,
    limit: Int
  )(jClient: DynamoDbAsyncClient): fs2.Stream[F, T] = {
    val query = Query[P](partitionKey)
    mkQueryRequestBuilder[F, P](
      index,
      keyExpression = query.keyCondition(index),
      filterExpression = query.filter,
      consistentRead = consistentRead,
      limit = limit
    ).flatMap(builder => sendQueryRequest[F, T](builder)(jClient))
  }

  private[meteor] def retrieveOp[
    F[_]: Async: RaiseThrowable,
    P: Encoder,
    S: Encoder,
    U: Decoder
  ](
    index: CompositeKeysIndex[P, S],
    query: Query[P, S],
    consistentRead: Boolean,
    limit: Int
  )(jClient: DynamoDbAsyncClient): fs2.Stream[F, U] = {
    mkQueryRequestBuilder[F, P](
      index,
      keyExpression = query.keysCondition(index),
      filterExpression = query.filter,
      consistentRead = consistentRead,
      limit = limit
    ).flatMap(builder => sendQueryRequest[F, U](builder)(jClient))
  }
}

private[meteor] trait SharedGetOps {
  private[meteor] def sendQueryRequest[F[_]: Async: RaiseThrowable, U: Decoder](
    builder: QueryRequest.Builder
  )(jClient: DynamoDbAsyncClient): fs2.Stream[F, U] = {
    def doQuery(
      req: QueryRequest
    ): fs2.Stream[F, QueryResponse] =
      fs2.Stream.eval(liftFuture(jClient.query(req))).flatMap { resp =>
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

  private[meteor] def sendGetRequest[F[_]: Async, U: Decoder](
    tableName: String,
    key: java.util.Map[String, AttributeValue],
    consistentRead: Boolean
  )(jClient: DynamoDbAsyncClient): F[Option[U]] = {
    val req =
      GetItemRequest.builder()
        .consistentRead(consistentRead)
        .tableName(tableName)
        .key(key)
        .build()
    liftFuture(jClient.getItem(req)).flatMap { resp =>
      if (resp.hasItem) {
        resp.item().asAttributeValue.as[U].map(_.some).liftTo[F]
      } else {
        none[U].pure[F]
      }
    }
  }

  private[meteor] def mkQueryRequestBuilder[F[_]: ApplicativeThrow, P](
    index: Index[P],
    keyExpression: Expression,
    filterExpression: Expression,
    consistentRead: Boolean,
    limit: Int
  ): fs2.Stream[F, QueryRequest.Builder] = {
    val (tableName, optIndexName) = index match {
      case PartitionKeyTable(name, _)     => (name, None)
      case CompositeKeysTable(name, _, _) => (name, None)
      case CompositeKeysSecondaryIndex(tableName, indexName, _, _) =>
        (tableName, indexName.some)
    }
    fs2.Stream.fromEither[F](
      if (keyExpression.nonEmpty) Right(keyExpression)
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
        if (filterExpression.isEmpty) {
          builder1
            .expressionAttributeNames(cond.attributeNames.asJava)
            .expressionAttributeValues(cond.attributeValues.asJava)
        } else {
          builder1.filterExpression(
            filterExpression.expression
          ).expressionAttributeNames(
            (cond.attributeNames ++ filterExpression.attributeNames).asJava
          ).expressionAttributeValues(
            (cond.attributeValues ++ filterExpression.attributeValues).asJava
          )
        }
    }
  }
}
