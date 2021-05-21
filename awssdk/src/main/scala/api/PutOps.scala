package meteor
package api

import cats.effect.Async
import cats.implicits._
import meteor.codec.{Decoder, Encoder}
import meteor.errors.ConditionalCheckFailed
import meteor.implicits._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.jdk.CollectionConverters._

private[meteor] trait PutOps {
  private[meteor] def putOp[F[_]: Async, T: Encoder](
    tableName: String,
    t: T,
    condition: Expression
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    val builder0 =
      PutItemRequest.builder()
        .tableName(tableName)
        .item(Encoder[T].write(t).m())
        .returnValues(ReturnValue.NONE)
    val builder =
      if (condition.isEmpty) {
        builder0
      } else {
        val builder1 = builder0
          .conditionExpression(condition.expression)
        val builder2 =
          if (condition.attributeValues.nonEmpty) {
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
    liftFuture(jClient.putItem(req)).void.adaptError {
      case err: ConditionalCheckFailedException =>
        ConditionalCheckFailed(err.getMessage)
    }
  }

  private[meteor] def putOp[F[_]: Async, T: Encoder, U: Decoder](
    tableName: String,
    t: T,
    condition: Expression
  )(jClient: DynamoDbAsyncClient): F[Option[U]] = {
    val builder0 =
      PutItemRequest.builder()
        .tableName(tableName)
        .item(Encoder[T].write(t).m())
        .returnValues(ReturnValue.ALL_OLD)
    val builder =
      if (condition.isEmpty) {
        builder0
      } else {
        val builder1 = builder0
          .conditionExpression(condition.expression)
        val builder2 =
          if (condition.attributeValues.nonEmpty) {
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
    liftFuture(jClient.putItem(req)).flatMap { resp =>
      if (resp.hasAttributes) {
        Async[F].fromEither(resp.attributes().asAttributeValue.as[U]).map(
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
