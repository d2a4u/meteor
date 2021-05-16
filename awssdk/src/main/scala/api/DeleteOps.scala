package meteor
package api

import cats.effect.Concurrent
import cats.implicits._
import meteor.codec.{Decoder, Encoder}
import meteor.implicits._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

private[meteor] trait DeleteOps
    extends PartitionKeyDeleteOps
    with CompositeKeysDeleteOps {}

private[meteor] trait CompositeKeysDeleteOps {
  private[meteor] def deleteOp[
    F[_]: Concurrent,
    P: Encoder,
    S: Encoder,
    U: Decoder
  ](
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
      (() => jClient.deleteItem(req)).liftF[F].flatMap { resp =>
        if (resp.hasAttributes) {
          Concurrent[F].fromEither(
            resp.attributes().asAttributeValue.as[U]
          ).map(_.some)
        } else {
          none[U].pure[F]
        }
      }
    }
  }
}

private[meteor] trait PartitionKeyDeleteOps {
  private[meteor] def deleteOp[F[_]: Concurrent, P: Encoder, U: Decoder](
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
      (() => jClient.deleteItem(req)).liftF[F].flatMap { resp =>
        if (resp.hasAttributes) {
          Concurrent[F].fromEither(
            resp.attributes().asAttributeValue.as[U]
          ).map(_.some)
        } else {
          none[U].pure[F]
        }
      }
    }
  }
}
