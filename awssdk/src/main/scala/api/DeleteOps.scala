package meteor
package api

import cats.effect.Concurrent
import cats.implicits._
import meteor.codec.Encoder
import meteor.implicits._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.jdk.CollectionConverters._

trait DeleteOps {
  def deleteOp[F[_]: Concurrent, P: Encoder, S: Encoder](
    table: Table,
    partitionKey: P,
    sortKey: S
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    val req =
      DeleteItemRequest.builder()
        .tableName(table.name)
        .key((Encoder[P].write(partitionKey).m().asScala ++ Encoder[S].write(
          sortKey
        ).m().asScala).asJava)
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
