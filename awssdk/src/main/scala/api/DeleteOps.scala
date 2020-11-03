package meteor
package api

import cats.effect.Concurrent
import cats.implicits._
import meteor.codec.Encoder
import meteor.implicits._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

trait DeleteOps {
  def deleteOp[F[_]: Concurrent, P: Encoder, S: Encoder](
    table: Table,
    partitionKey: P,
    sortKey: S
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    val req =
      DeleteItemRequest.builder()
        .tableName(table.name)
        .key(Encoder[(P, S)].write((partitionKey, sortKey)).m())
        .build()
    (() => jClient.deleteItem(req)).liftF[F].void
  }

  def deleteOp[F[_]: Concurrent, P: Encoder](
    table: Table,
    partitionKey: P
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    val req =
      DeleteItemRequest.builder()
        .tableName(table.name)
        .key(Encoder[P].write(partitionKey).m())
        .build()
    (() => jClient.deleteItem(req)).liftF[F].void
  }
}
