package meteor
package api

import cats.effect.Concurrent
import cats.implicits._
import meteor.codec.{Decoder, Encoder}
import meteor.errors.ConditionalCheckFailed
import meteor.implicits._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.jdk.CollectionConverters._

trait UpdateOps extends PartitionKeyUpdateOps with CompositeKeysUpdateOps {}

trait PartitionKeyUpdateOps extends SharedUpdateOps {
  def updateOp[F[_]: Concurrent, P: Encoder, U: Decoder](
    table: PartitionKeyTable[P],
    partitionKey: P,
    update: Expression,
    returnValue: ReturnValue
  )(jClient: DynamoDbAsyncClient): F[Option[U]] = {
    table.mkKey[F](partitionKey).flatMap { key =>
      val req =
        mkUpdateRequestBuilder(table.tableName, update, returnValue).key(
          key
        ).build()
      sendUpdateItem[F, U](req)(jClient)
    }
  }

  def updateOp[F[_]: Concurrent, P: Encoder, U: Decoder](
    table: PartitionKeyTable[P],
    partitionKey: P,
    update: Expression,
    condition: Expression,
    returnValue: ReturnValue
  )(jClient: DynamoDbAsyncClient): F[Option[U]] = {
    table.mkKey[F](partitionKey).flatMap { key =>
      val req =
        mkUpdateRequestBuilder(
          table.tableName,
          update,
          condition,
          returnValue
        ).key(
          key
        ).build()
      sendUpdateItem[F, U](req)(jClient)
    }
  }

  def updateOp[F[_]: Concurrent, P: Encoder](
    table: PartitionKeyTable[P],
    partitionKey: P,
    update: Expression
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    table.mkKey[F](partitionKey).flatMap { key =>
      val req =
        mkUpdateRequestBuilder(table.tableName, update, ReturnValue.NONE).key(
          key
        ).build()
      sendUpdateItem[F](req)(jClient)
    }
  }

  def updateOp[F[_]: Concurrent, P: Encoder](
    table: PartitionKeyTable[P],
    partitionKey: P,
    update: Expression,
    condition: Expression
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    table.mkKey[F](partitionKey).flatMap { key =>
      val req =
        mkUpdateRequestBuilder(
          table.tableName,
          update,
          condition,
          ReturnValue.NONE
        ).key(
          key
        ).build()
      sendUpdateItem[F](req)(jClient)
    }
  }

}

trait CompositeKeysUpdateOps extends SharedUpdateOps {
  def updateOp[F[_]: Concurrent, P: Encoder, S: Encoder, U: Decoder](
    table: CompositeKeysTable[P, S],
    partitionKey: P,
    sortKey: S,
    update: Expression,
    returnValue: ReturnValue
  )(jClient: DynamoDbAsyncClient): F[Option[U]] = {
    table.mkKey[F](partitionKey, sortKey).flatMap { key =>
      val req =
        mkUpdateRequestBuilder(table.tableName, update, returnValue).key(
          key
        ).build()
      sendUpdateItem[F, U](req)(jClient)
    }
  }

  def updateOp[F[_]: Concurrent, P: Encoder, S: Encoder, U: Decoder](
    table: CompositeKeysTable[P, S],
    partitionKey: P,
    sortKey: S,
    update: Expression,
    condition: Expression,
    returnValue: ReturnValue
  )(jClient: DynamoDbAsyncClient): F[Option[U]] = {
    table.mkKey[F](partitionKey, sortKey).flatMap { key =>
      val req =
        mkUpdateRequestBuilder(
          table.tableName,
          update,
          condition,
          returnValue
        ).key(
          key
        ).build()
      sendUpdateItem[F, U](req)(jClient)
    }
  }

  def updateOp[F[_]: Concurrent, P: Encoder, S: Encoder](
    table: CompositeKeysTable[P, S],
    partitionKey: P,
    sortKey: S,
    update: Expression
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    table.mkKey[F](partitionKey, sortKey).flatMap { key =>
      val req =
        mkUpdateRequestBuilder(table.tableName, update, ReturnValue.NONE).key(
          key
        ).build()
      sendUpdateItem[F](req)(jClient)
    }
  }

  def updateOp[F[_]: Concurrent, P: Encoder, S: Encoder](
    table: CompositeKeysTable[P, S],
    partitionKey: P,
    sortKey: S,
    update: Expression,
    condition: Expression
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    table.mkKey[F](partitionKey, sortKey).flatMap { key =>
      val req =
        mkUpdateRequestBuilder(
          table.tableName,
          update,
          condition,
          ReturnValue.NONE
        ).key(
          key
        ).build()
      sendUpdateItem[F](req)(jClient)
    }
  }

}

trait SharedUpdateOps {
  def sendUpdateItem[F[_]: Concurrent](req: UpdateItemRequest)(
    jClient: DynamoDbAsyncClient
  ): F[Unit] =
    (() => jClient.updateItem(req)).liftF[F].adaptError {
      case err: ConditionalCheckFailedException =>
        ConditionalCheckFailed(err.getMessage)
    }.void

  def sendUpdateItem[F[_]: Concurrent, U: Decoder](
    req: UpdateItemRequest
  )(jClient: DynamoDbAsyncClient): F[Option[U]] =
    (() => jClient.updateItem(req)).liftF[F].flatMap { resp =>
      if (resp.hasAttributes) {
        Concurrent[F].fromEither(
          resp.attributes().asAttributeValue.as[U].map(_.some)
        )
      } else {
        none[U].pure[F]
      }

    }.adaptError {
      case err: ConditionalCheckFailedException =>
        ConditionalCheckFailed(err.getMessage)
    }

  def mkUpdateRequestBuilder(
    tableName: String,
    update: Expression,
    condition: Expression,
    returnValue: ReturnValue
  ): UpdateItemRequest.Builder =
    UpdateItemRequest.builder()
      .tableName(tableName)
      .updateExpression(update.expression)
      .conditionExpression(condition.expression)
      .expressionAttributeNames(
        (update.attributeNames ++ condition.attributeNames).asJava
      )
      .expressionAttributeValues(
        (update.attributeValues ++ condition.attributeValues).asJava
      )
      .returnValues(returnValue)

  def mkUpdateRequestBuilder(
    tableName: String,
    update: Expression,
    returnValue: ReturnValue
  ): UpdateItemRequest.Builder =
    UpdateItemRequest.builder()
      .tableName(tableName)
      .updateExpression(update.expression)
      .expressionAttributeNames(update.attributeNames.asJava)
      .expressionAttributeValues(update.attributeValues.asJava)
      .returnValues(returnValue)
}
