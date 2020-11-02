package meteor
package api

import cats.effect.{Concurrent, Timer}
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

  private def sendHandleLeftOver[F[_]: Concurrent: Timer](
    req: BatchWriteItemRequest
  )(
    jClient: DynamoDbAsyncClient
  ): Stream[F, BatchWriteItemResponse] =
    Stream.eval(
      (() => jClient.batchWriteItem(req)).liftF[F]
    ).flatMap { resp =>
      Stream.emit(resp) ++ {
        if (resp.hasUnprocessedItems && !resp.unprocessedItems().isEmpty) {
          val nextReq = BatchWriteItemRequest.builder().requestItems(
            resp.unprocessedItems()
          ).build()
          sendHandleLeftOver(nextReq)(jClient)
        } else {
          Stream.empty
        }
      }
    }

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

  private def mkRequest[F[_]: Timer: Concurrent, D: Encoder, P: Encoder](
    table: Table,
    maxBatchWait: FiniteDuration,
    rightIsWrite: Boolean
  ): Pipe[F, Either[D, P], BatchWriteItemRequest] =
    _.groupWithin(25, maxBatchWait).map { chunk =>
      def mkWriteRequest(item: Either[D, P]): WriteRequest = {
        val av = Encoder[Either[D, P]].write(item).m()
        item match {
          case Left(_) if rightIsWrite =>
            val del = DeleteRequest.builder().key(av).build()
            WriteRequest.builder().deleteRequest(del).build()

          case Left(_) if !rightIsWrite =>
            val put = PutRequest.builder().item(av).build()
            WriteRequest.builder().putRequest(put).build()

          case Right(_) if rightIsWrite =>
            val put = PutRequest.builder().item(av).build()
            WriteRequest.builder().putRequest(put).build()

          case Right(_) if !rightIsWrite =>
            val del = DeleteRequest.builder().key(av).build()
            WriteRequest.builder().deleteRequest(del).build()

        }
      }

      val writes =
        Map(table.name -> chunk.map(mkWriteRequest).toList.asJava).asJava
      BatchWriteItemRequest.builder().requestItems(
        writes
      ).build()
    }

  private def sendPipeUnordered[F[_]: Concurrent: Timer, T: Encoder](
    table: Table,
    maxBatchWait: FiniteDuration,
    parallelism: Int
  )(jClient: DynamoDbAsyncClient): Pipe[F, Write[T], BatchWriteItemResponse] =
    in =>
      mkRequest[F, T](table, maxBatchWait).apply(in).map { req =>
        sendHandleLeftOver(req)(jClient)
      }.parJoin(parallelism)

  private def sendPipeInordered[
    F[_]: Concurrent: Timer,
    D: Encoder,
    P: Encoder
  ](
    table: Table,
    maxBatchWait: FiniteDuration,
    rightIsWrite: Boolean = true
  )(jClient: DynamoDbAsyncClient)
    : Pipe[F, Either[D, P], BatchWriteItemResponse] =
    in =>
      mkRequest[F, D, P](table, maxBatchWait, rightIsWrite).apply(in).flatMap {
        req =>
          sendHandleLeftOver(req)(jClient)
      }

  def batchWriteUnorderedOp[F[_]: Timer: Concurrent, T: Encoder](
    table: Table,
    maxBatchWait: FiniteDuration,
    parallelism: Int
  )(jClient: DynamoDbAsyncClient): Pipe[F, Write[T], Unit] =
    sendPipeUnordered[F, T](table, maxBatchWait, parallelism)(jClient).andThen(
      _.drain
    )

  def batchWriteInorderedOp[F[_]: Timer: Concurrent, D: Encoder, P: Encoder](
    table: Table,
    maxBatchWait: FiniteDuration,
    rightIsWrite: Boolean = true
  )(jClient: DynamoDbAsyncClient): Pipe[F, Either[D, P], Unit] =
    sendPipeInordered[F, D, P](table, maxBatchWait, rightIsWrite)(
      jClient
    ).andThen(_.drain)
}
