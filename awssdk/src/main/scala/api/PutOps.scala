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

trait PutOps {
  def putOp[F[_]: Concurrent, T: Encoder](
    table: Table,
    t: T
  )(jClient: DynamoDbAsyncClient): F[Unit] =
    putOp[F, T](table, t, Expression.empty)(jClient)

  def putOp[F[_]: Concurrent, T: Encoder](
    table: Table,
    t: T,
    condition: Expression
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    val builder0 =
      PutItemRequest.builder()
        .tableName(table.name)
        .item(Encoder[T].write(t).m())
        .returnValues(ReturnValue.NONE)
    val builder = if (condition.isEmpty) {
      builder0
    } else {
      val builder1 = builder0
        .conditionExpression(condition.expression)
      val builder2 = if (condition.attributeValues.nonEmpty) {
        builder0
          .expressionAttributeValues(condition.attributeValues.asJava)
      } else {
        builder1
      }
      if (condition.attributeNames.nonEmpty) {
        builder2.expressionAttributeNames(condition.attributeNames.asJava)
      } else {
        builder2
      }
    }
    val req = builder.build()
    (() => jClient.putItem(req)).liftF[F].void.adaptError {
      case err: ConditionalCheckFailedException =>
        ConditionalCheckFailed(err.getMessage)
    }
  }

  def putOp[F[_]: Concurrent, T: Encoder, U: Decoder](
    table: Table,
    t: T
  )(jClient: DynamoDbAsyncClient): F[Option[U]] =
    putOp[F, T, U](table, t, Expression.empty)(jClient)

  def putOp[F[_]: Concurrent, T: Encoder, U: Decoder](
    table: Table,
    t: T,
    condition: Expression
  )(jClient: DynamoDbAsyncClient): F[Option[U]] = {
    val builder0 =
      PutItemRequest.builder()
        .tableName(table.name)
        .item(Encoder[T].write(t).m())
        .returnValues(ReturnValue.ALL_OLD)
    val builder = if (condition.isEmpty) {
      builder0
    } else {
      val builder1 = builder0
        .conditionExpression(condition.expression)
      val builder2 = if (condition.attributeValues.nonEmpty) {
        builder0
          .expressionAttributeValues(condition.attributeValues.asJava)
      } else {
        builder1
      }
      if (condition.attributeNames.nonEmpty) {
        builder2.expressionAttributeNames(condition.attributeNames.asJava)
      } else {
        builder2
      }
    }
    val req = builder.build()
    (() => jClient.putItem(req)).liftF[F].flatMap { resp =>
      if (resp.hasAttributes()) {
        Concurrent[F].fromEither(resp.attributes().asAttributeValue.as[U]).map(
          _.some
        )
      } else {
        none[U].pure[F]
      }
    }.adaptError {
      case err: ConditionalCheckFailedException =>
        ConditionalCheckFailed(err.getMessage)
    }
  }
}
