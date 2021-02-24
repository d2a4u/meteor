package meteor
package api

import cats.effect.Concurrent
import cats.implicits._
import meteor.codec.Encoder
import meteor.implicits._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

trait DeleteOps extends PartitionKeyDeleteOps with CompositeKeysDeleteOps {}

trait CompositeKeysDeleteOps {
  def deleteOp[F[_]: Concurrent, P: Encoder, S: Encoder](
    table: CompositeKeysTable[P, S],
    partitionKey: P,
    sortKey: S
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    table.mkKey[F](partitionKey, sortKey).flatMap { key =>
      val req =
        DeleteItemRequest.builder()
          .tableName(table.tableName)
          .key(key)
          .build()
      (() => jClient.deleteItem(req)).liftF[F].void
    }
  }
}

trait PartitionKeyDeleteOps {
  def deleteOp[F[_]: Concurrent, P: Encoder](
    table: PartitionKeyTable[P],
    partitionKey: P
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    table.mkKey[F](partitionKey).flatMap { key =>
      val req =
        DeleteItemRequest.builder()
          .tableName(table.tableName)
          .key(key)
          .build()
      (() => jClient.deleteItem(req)).liftF[F].void
    }
  }
}
