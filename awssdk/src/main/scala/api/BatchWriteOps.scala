package meteor
package api

import cats.effect.{Concurrent, Timer}
import cats.implicits._
import fs2.{Pipe, Stream}
import meteor.codec.Encoder
import meteor.implicits._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

trait BatchWriteOps {
  sealed trait Write[T] {
    def item: T
  }

  case class Deletion[T](item: T) extends Write[T]
  case class Put[T](item: T) extends Write[T]

  private def loop[F[_]: Concurrent: Timer](
    req: BatchWriteItemRequest,
    parallelism: Int
  )(
    jClient: DynamoDbAsyncClient
  ): Stream[F, BatchWriteItemResponse] =
    Stream.eval(
      (() => jClient.batchWriteItem(req)).liftF[F]
    ).map { resp =>
      if (resp.hasUnprocessedItems && !resp.unprocessedItems().isEmpty) {
        val req0 = BatchWriteItemRequest.builder().requestItems(
          resp.unprocessedItems()
        ).build()
        Stream.emit(resp) ++ loop(req0, parallelism)(jClient)
      } else {
        Stream.emit[F, BatchWriteItemResponse](resp)
      }
    }.parJoin(parallelism)

  //one batch can be max 25 items
  private def mkRequest[F[_]: Timer: Concurrent, T: Encoder](
    table: Table,
    maxBatchWait: FiniteDuration
  ): Pipe[F, Write[T], BatchWriteItemRequest] =
    _.groupWithin(25, maxBatchWait).map { chunk =>
      val writes = Map(table.name -> chunk.map {
        case Deletion(t) =>
          val keys = Encoder[T].write(t).m()
          val del = DeleteRequest.builder().key(keys).build()
          WriteRequest.builder().deleteRequest(del).build()

        case Put(t) =>
          val item = Encoder[T].write(t).m()
          val put = PutRequest.builder().item(item).build()
          WriteRequest.builder().putRequest(put).build()
      }.toList.asJava).asJava
      BatchWriteItemRequest.builder().requestItems(
        writes
      ).build()
    }

  private def sendPipe[F[_]: Concurrent: Timer, T: Encoder](
    table: Table,
    maxBatchWait: FiniteDuration,
    parallelism: Int
  )(jClient: DynamoDbAsyncClient): Pipe[F, Write[T], BatchWriteItemResponse] =
    in =>
      mkRequest[F, T](table, maxBatchWait).apply(in).map {
        req =>
          loop(req, parallelism)(jClient)
      }.parJoin(parallelism)

  def batchWriteOp[F[_]: Timer: Concurrent, T: Encoder](
    table: Table,
    maxBatchWait: FiniteDuration,
    parallelism: Int
  )(jClient: DynamoDbAsyncClient): Pipe[F, Write[T], Unit] =
    sendPipe[F, T](table, maxBatchWait, parallelism)(jClient).andThen(_.void)
}
