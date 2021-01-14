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

trait UpdateOps {
  def updateOp[F[_]: Concurrent, P: Encoder, U: Decoder](
    table: Table,
    partitionKey: P,
    update: Expression,
    returnValue: ReturnValue
  )(jClient: DynamoDbAsyncClient): F[Option[U]] = {
    val req =
      mkBuilder(table.name, update, returnValue).key(table.keys(
        partitionKey,
        None
      )).build()
    sendUpdateItem[F, U](req)(jClient)
  }

  def updateOp[F[_]: Concurrent, P: Encoder, U: Decoder](
    table: Table,
    partitionKey: P,
    update: Expression,
    condition: Expression,
    returnValue: ReturnValue
  )(jClient: DynamoDbAsyncClient): F[Option[U]] = {
    val req =
      mkBuilder(table.name, update, condition, returnValue).key(table.keys(
        partitionKey,
        None
      )).build()
    sendUpdateItem[F, U](req)(jClient)
  }

  def updateOp[F[_]: Concurrent, P: Encoder, S: Encoder, U: Decoder](
    table: Table,
    partitionKey: P,
    sortKey: S,
    update: Expression,
    returnValue: ReturnValue
  )(jClient: DynamoDbAsyncClient): F[Option[U]] = {
    val req =
      mkBuilder(table.name, update, returnValue).key(table.keys(
        partitionKey,
        sortKey.some
      )).build()
    sendUpdateItem[F, U](req)(jClient)
  }

  def updateOp[F[_]: Concurrent, P: Encoder, S: Encoder, U: Decoder](
    table: Table,
    partitionKey: P,
    sortKey: S,
    update: Expression,
    condition: Expression,
    returnValue: ReturnValue
  )(jClient: DynamoDbAsyncClient): F[Option[U]] = {
    val req =
      mkBuilder(table.name, update, condition, returnValue).key(table.keys(
        partitionKey,
        sortKey.some
      )).build()
    sendUpdateItem[F, U](req)(jClient)
  }

  def updateOp[F[_]: Concurrent, P: Encoder](
    table: Table,
    partitionKey: P,
    update: Expression
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    val req =
      mkBuilder(table.name, update, ReturnValue.NONE).key(table.keys(
        partitionKey,
        None
      )).build()
    sendUpdateItem[F](req)(jClient)
  }

  def updateOp[F[_]: Concurrent, P: Encoder](
    table: Table,
    partitionKey: P,
    update: Expression,
    condition: Expression
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    val req =
      mkBuilder(table.name, update, condition, ReturnValue.NONE).key(table.keys(
        partitionKey,
        None
      )).build()
    sendUpdateItem[F](req)(jClient)
  }

  def updateOp[F[_]: Concurrent, P: Encoder, S: Encoder](
    table: Table,
    partitionKey: P,
    sortKey: S,
    update: Expression
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    val req =
      mkBuilder(table.name, update, ReturnValue.NONE).key(table.keys(
        partitionKey,
        sortKey.some
      )).build()
    sendUpdateItem[F](req)(jClient)
  }

  def updateOp[F[_]: Concurrent, P: Encoder, S: Encoder](
    table: Table,
    partitionKey: P,
    sortKey: S,
    update: Expression,
    condition: Expression
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    val req =
      mkBuilder(table.name, update, condition, ReturnValue.NONE).key(table.keys(
        partitionKey,
        sortKey.some
      )).build()
    sendUpdateItem[F](req)(jClient)
  }

  private def sendUpdateItem[F[_]: Concurrent](req: UpdateItemRequest)(
    jClient: DynamoDbAsyncClient
  ): F[Unit] =
    (() => jClient.updateItem(req)).liftF[F].adaptError {
      case err: ConditionalCheckFailedException =>
        ConditionalCheckFailed(err.getMessage)
    }.void

  private def sendUpdateItem[F[_]: Concurrent, U: Decoder](
    req: UpdateItemRequest
  )(jClient: DynamoDbAsyncClient): F[Option[U]] =
    (() => jClient.updateItem(req)).liftF[F].flatMap { resp =>
      if (resp.hasAttributes()) {
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

  private def mkBuilder(
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

  private def mkBuilder(
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
