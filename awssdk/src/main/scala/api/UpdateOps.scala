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
      withKey(mkBuilder(table, update, returnValue))(
        partitionKey
      ).build()
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
      withKey(mkBuilder(table, update, condition, returnValue))(
        partitionKey
      ).build()
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
      withKeys(mkBuilder(table, update, returnValue))(
        partitionKey,
        sortKey
      ).build()
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
      withKeys(mkBuilder(table, update, condition, returnValue))(
        partitionKey,
        sortKey
      ).build()
    sendUpdateItem[F, U](req)(jClient)
  }

  def updateOp[F[_]: Concurrent, P: Encoder](
    table: Table,
    partitionKey: P,
    update: Expression
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    val req =
      withKey(mkBuilder(table, update, ReturnValue.NONE))(
        partitionKey
      ).build()
    sendUpdateItem[F](req)(jClient)
  }

  def updateOp[F[_]: Concurrent, P: Encoder](
    table: Table,
    partitionKey: P,
    update: Expression,
    condition: Expression
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    val req =
      withKey(mkBuilder(table, update, condition, ReturnValue.NONE))(
        partitionKey
      ).build()
    sendUpdateItem[F](req)(jClient)
  }

  def updateOp[F[_]: Concurrent, P: Encoder, S: Encoder](
    table: Table,
    partitionKey: P,
    sortKey: S,
    update: Expression
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    val req =
      withKeys(mkBuilder(table, update, ReturnValue.NONE))(
        partitionKey,
        sortKey
      ).build()
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
      withKeys(mkBuilder(table, update, condition, ReturnValue.NONE))(
        partitionKey,
        sortKey
      ).build()
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
      Concurrent[F].fromEither(resp.attributes().attemptDecode[U])
    }.adaptError {
      case err: ConditionalCheckFailedException =>
        ConditionalCheckFailed(err.getMessage)
    }

  private def withKeys[P: Encoder, S: Encoder](
    builder: UpdateItemRequest.Builder
  )(partitionKey: P, sortKey: S): UpdateItemRequest.Builder = {
    builder.key(Encoder[(P, S)].write((partitionKey, sortKey)).m())
  }

  private def withKey[P: Encoder](
    builder: UpdateItemRequest.Builder
  )(partitionKey: P): UpdateItemRequest.Builder =
    builder.key(Encoder[P].write(partitionKey).m())

  private def mkBuilder(
    table: Table,
    update: Expression,
    condition: Expression,
    returnValue: ReturnValue
  ): UpdateItemRequest.Builder =
    UpdateItemRequest.builder()
      .tableName(table.name)
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
    table: Table,
    update: Expression,
    returnValue: ReturnValue
  ): UpdateItemRequest.Builder =
    UpdateItemRequest.builder()
      .tableName(table.name)
      .updateExpression(update.expression)
      .expressionAttributeNames(update.attributeNames.asJava)
      .expressionAttributeValues(update.attributeValues.asJava)
      .returnValues(returnValue)
}
