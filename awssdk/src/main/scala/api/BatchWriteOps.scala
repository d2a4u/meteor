package meteor
package api

import java.util
import java.util.{Map => jMap}
import cats.effect.{Concurrent, Timer}
import fs2.{Pipe, Stream}
import meteor.codec.Encoder
import meteor.implicits._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

trait BatchWriteOps extends DedupOps {
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
  private def mkRequestOutOrdered[F[_]: Timer: Concurrent, T: Encoder](
    tableName: String,
    maxBatchWait: FiniteDuration
  ): Pipe[F, Write[T], BatchWriteItemRequest] =
    _.groupWithin(25, maxBatchWait).map { chunk =>
      val reqs =
        chunk.foldLeft(Map.empty[Write[T], jMap[String, AttributeValue]]) {
          (acc, write) =>
            acc + (write -> Encoder[T].write(write.item).m())
        }.map {
          case (Deletion(_), key) =>
            val del = DeleteRequest.builder().key(key).build()
            WriteRequest.builder().deleteRequest(del).build()

          case (Put(_), item) =>
            val put = PutRequest.builder().item(item).build()
            WriteRequest.builder().putRequest(put).build()
        }.toList.asJava

      val writes = Map(tableName -> reqs).asJava
      BatchWriteItemRequest.builder().requestItems(writes).build()
    }

  private def mkRequestInOrdered[
    F[_]: Timer: Concurrent,
    D: Encoder,
    P: Encoder
  ](
    table: Table,
    maxBatchWait: FiniteDuration
  ): Pipe[F, Either[D, P], BatchWriteItemRequest] =
    _.groupWithin(25, maxBatchWait).map { chunk =>
      def mkWriteRequest(item: Either[D, P]): WriteRequest = {
        val av = Encoder[Either[D, P]].write(item).m()
        item match {
          case Left(_) =>
            val del = DeleteRequest.builder().key(av).build()
            WriteRequest.builder().deleteRequest(del).build()

          case Right(_) =>
            val put = PutRequest.builder().item(av).build()
            WriteRequest.builder().putRequest(put).build()
        }
      }

      val writes =
        Map(
          table.name -> dedupInOrdered(chunk)(getKeys(table))(
            mkWriteRequest
          ).asJava
        ).asJava
      BatchWriteItemRequest.builder().requestItems(
        writes
      ).build()
    }

  private def mkRequestInOrdered[
    F[_]: Timer: Concurrent,
    P: Encoder
  ](
    table: Table,
    maxBatchWait: FiniteDuration
  ): Pipe[F, P, BatchWriteItemRequest] =
    _.groupWithin(25, maxBatchWait).map { chunk =>

      def mkWriteRequest(item: P): WriteRequest = {
        val av = Encoder[P].write(item).m()
        val put = PutRequest.builder().item(av).build()
        WriteRequest.builder().putRequest(put).build()
      }

      val writes =
        Map(
          table.name -> dedupInOrdered(chunk)(getKeys(table))(
            mkWriteRequest
          ).asJava
        ).asJava
      BatchWriteItemRequest.builder().requestItems(
        writes
      ).build()
    }

  private def sendPipeOutOrdered[F[_]: Concurrent: Timer, T: Encoder](
    tableName: String,
    maxBatchWait: FiniteDuration,
    parallelism: Int
  )(jClient: DynamoDbAsyncClient): Pipe[F, Write[T], BatchWriteItemResponse] =
    in =>
      mkRequestOutOrdered[F, T](tableName, maxBatchWait).apply(in).map { req =>
        sendHandleLeftOver(req)(jClient)
      }.parJoin(parallelism)

  private def sendPipeInOrdered[
    F[_]: Concurrent: Timer,
    D: Encoder,
    P: Encoder
  ](
    table: Table,
    maxBatchWait: FiniteDuration
  )(jClient: DynamoDbAsyncClient)
    : Pipe[F, Either[D, P], BatchWriteItemResponse] =
    in =>
      mkRequestInOrdered[F, D, P](table, maxBatchWait).apply(
        in
      ).flatMap {
        req =>
          sendHandleLeftOver(req)(jClient)
      }

  private def sendPipeInOrdered[
    F[_]: Concurrent: Timer,
    P: Encoder
  ](
    table: Table,
    maxBatchWait: FiniteDuration
  )(jClient: DynamoDbAsyncClient): Pipe[F, P, BatchWriteItemResponse] =
    in =>
      mkRequestInOrdered[F, P](table, maxBatchWait).apply(
        in
      ).flatMap {
        req =>
          sendHandleLeftOver(req)(jClient)
      }

  private def getKeys[T: Encoder](table: Table)(t: T): AttributeValue = {
    val av = Encoder[T].write(t)
    if (av.hasM) {
      val m = av.m()
      val partitionKey = new util.HashMap[String, AttributeValue]()
      partitionKey.put(
        table.hashKey.name,
        m.get(table.hashKey.name)
      )
      val keys =
        table.rangeKey.fold[jMap[String, AttributeValue]](partitionKey) { key =>
          val sortKey = new util.HashMap[String, AttributeValue]()
          sortKey.put(key.name, m.get(key.name))
          sortKey ++ partitionKey
        }
      AttributeValue.builder().m(keys).build()
    } else {
      AttributeValue.builder().nul(true).build()
    }
  }

  def batchDeleteUnorderedOp[F[_]: Timer: Concurrent, D: Encoder](
    tableName: String,
    maxBatchWait: FiniteDuration,
    parallelism: Int
  )(jClient: DynamoDbAsyncClient): Pipe[F, D, Unit] =
    in => {
      val pipe = sendPipeOutOrdered[F, D](tableName, maxBatchWait, parallelism)(
        jClient
      ).andThen(
        _.drain
      )
      in.map(Deletion(_)).through(pipe)
    }

  def batchPutInorderedOp[F[_]: Timer: Concurrent, P: Encoder](
    table: Table,
    maxBatchWait: FiniteDuration
  )(jClient: DynamoDbAsyncClient): Pipe[F, P, Unit] =
    sendPipeInOrdered[F, P](table, maxBatchWait)(jClient).andThen(
      _.drain
    )

  def batchPutUnorderedOp[F[_]: Timer: Concurrent, P: Encoder](
    tableName: String,
    maxBatchWait: FiniteDuration,
    parallelism: Int
  )(jClient: DynamoDbAsyncClient): Pipe[F, P, Unit] =
    in => {
      val pipe = sendPipeOutOrdered[F, P](tableName, maxBatchWait, parallelism)(
        jClient
      ).andThen(
        _.drain
      )
      in.map(Put(_)).through(pipe)
    }

  def batchWriteInorderedOp[F[_]: Timer: Concurrent, D: Encoder, P: Encoder](
    table: Table,
    maxBatchWait: FiniteDuration
  )(jClient: DynamoDbAsyncClient): Pipe[F, Either[D, P], Unit] =
    sendPipeInOrdered[F, D, P](table, maxBatchWait)(
      jClient
    ).andThen(_.drain)
}

object BatchWriteOps extends BatchWriteOps
