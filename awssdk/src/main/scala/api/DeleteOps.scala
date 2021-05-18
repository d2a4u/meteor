package meteor
package api

import cats.effect.Async
import cats.implicits._
import meteor.codec.{Decoder, Encoder}
import meteor.implicits._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

trait DeleteOps extends PartitionKeyDeleteOps with CompositeKeysDeleteOps {}

trait CompositeKeysDeleteOps {
  def deleteOp[F[_]: Async, P: Encoder, S: Encoder, U: Decoder](
    table: CompositeKeysTable[P, S],
    partitionKey: P,
    sortKey: S,
    returnValue: ReturnValue
  )(jClient: DynamoDbAsyncClient): F[Option[U]] = {
    table.mkKey[F](partitionKey, sortKey).flatMap { key =>
      val req =
        DeleteItemRequest.builder()
          .tableName(table.tableName)
          .key(key)
          .returnValues(returnValue)
          .build()
      liftFuture(jClient.deleteItem(req)).flatMap { resp =>
        if (resp.hasAttributes) {
          Async[F].fromEither(
            resp.attributes().asAttributeValue.as[U]
          ).map(_.some)
        } else {
          none[U].pure[F]
        }
      }
    }
  }
}

trait PartitionKeyDeleteOps {
  def deleteOp[F[_]: Async, P: Encoder, U: Decoder](
    table: PartitionKeyTable[P],
    partitionKey: P,
    returnValue: ReturnValue
  )(jClient: DynamoDbAsyncClient): F[Option[U]] = {
    table.mkKey[F](partitionKey).flatMap { key =>
      val req =
        DeleteItemRequest.builder()
          .tableName(table.tableName)
          .key(key)
          .returnValues(returnValue)
          .build()
      liftFuture(jClient.deleteItem(req)).flatMap { resp =>
        if (resp.hasAttributes) {
          Async[F].fromEither(
            resp.attributes().asAttributeValue.as[U]
          ).map(_.some)
        } else {
          none[U].pure[F]
        }
      }
    }
  }
}
