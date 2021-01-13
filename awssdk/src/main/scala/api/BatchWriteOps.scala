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

  val MaxBatchWriteSize = 25

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

  private def mkDeleteRequestOutOrdered[F[_]: Timer: Concurrent, P: Encoder](
    table: Table,
    maxBatchWait: FiniteDuration
  ): Pipe[F, P, BatchWriteItemRequest] =
    _.groupWithin(MaxBatchWriteSize, maxBatchWait).map { chunk =>
      val reqs =
        chunk.foldLeft(Map.empty[P, jMap[String, AttributeValue]]) {
          (acc, partitionKey) =>
            acc + (partitionKey -> table.keys(partitionKey, None))
        }.map {
          case (_, key) =>
            val del = DeleteRequest.builder().key(key).build()
            WriteRequest.builder().deleteRequest(del).build()
        }.toList.asJava

      val writes = Map(table.name -> reqs).asJava
      BatchWriteItemRequest.builder().requestItems(writes).build()
    }

  private def mkDeleteRequestOutOrdered[
    F[_]: Timer: Concurrent,
    P: Encoder,
    S: Encoder
  ](
    table: Table,
    maxBatchWait: FiniteDuration
  ): Pipe[F, (P, S), BatchWriteItemRequest] =
    _.groupWithin(MaxBatchWriteSize, maxBatchWait).map { chunk =>
      val reqs =
        chunk.foldLeft(Map.empty[(P, S), jMap[String, AttributeValue]]) {
          (acc, ps) =>
            acc + (ps -> table.keys(ps._1, Some(ps._2)))
        }.map {
          case (_, key) =>
            val del = DeleteRequest.builder().key(key).build()
            WriteRequest.builder().deleteRequest(del).build()
        }.toList.asJava

      val writes = Map(table.name -> reqs).asJava
      BatchWriteItemRequest.builder().requestItems(writes).build()
    }

  private def mkRequestInOrdered[
    F[_]: Timer: Concurrent,
    P: Encoder,
    I: Encoder
  ](
    table: Table,
    maxBatchWait: FiniteDuration
  ): Pipe[F, Either[P, I], BatchWriteItemRequest] =
    _.groupWithin(MaxBatchWriteSize, maxBatchWait).map { chunk =>
      def mkWriteRequest(item: Either[P, I]): WriteRequest = {
        item match {
          case Left(p) =>
            val key = table.keys[P, Nothing](p, None)
            val del = DeleteRequest.builder().key(key).build()
            WriteRequest.builder().deleteRequest(del).build()

          case Right(i) =>
            val put = PutRequest.builder().item(i.asAttributeValue.m()).build()
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
    P: Encoder,
    S: Encoder,
    I: Encoder
  ](
    table: Table,
    maxBatchWait: FiniteDuration
  ): Pipe[F, Either[(P, S), I], BatchWriteItemRequest] =
    _.groupWithin(MaxBatchWriteSize, maxBatchWait).map { chunk =>
      def mkWriteRequest(item: Either[(P, S), I]): WriteRequest = {
        item match {
          case Left((p, s)) =>
            val del =
              DeleteRequest.builder().key(table.keys(p, Some(s))).build()
            WriteRequest.builder().deleteRequest(del).build()

          case Right(i) =>
            val put = PutRequest.builder().item(i.asAttributeValue.m()).build()
            WriteRequest.builder().putRequest(put).build()
        }
      }
      val itemEncoder = Encoder.instance[Either[(P, S), I]] {
        case Left((p, s)) =>
          table.keys[P, S](p, Some(s)).asAttributeValue

        case Right(i) =>
          i.asAttributeValue
      }
      val writes =
        Map(
          table.name -> dedupInOrdered(chunk)(item =>
            getKeys(table)(item)(itemEncoder))(
            mkWriteRequest
          ).asJava
        ).asJava
      BatchWriteItemRequest.builder().requestItems(
        writes
      ).build()
    }

  private def mkPutRequestInOrdered[
    F[_]: Timer: Concurrent,
    I: Encoder
  ](
    table: Table,
    maxBatchWait: FiniteDuration
  ): Pipe[F, I, BatchWriteItemRequest] =
    _.groupWithin(MaxBatchWriteSize, maxBatchWait).map { chunk =>

      def mkWriteRequest(item: I): WriteRequest = {
        val av = item.asAttributeValue.m()
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

  private def getKeys[T: Encoder](table: Table)(t: T): AttributeValue = {
    val av = t.asAttributeValue
    if (av.hasM) {
      val m = av.m()
      val partitionKey = new util.HashMap[String, AttributeValue]()
      partitionKey.put(
        table.partitionKey.name,
        m.get(table.partitionKey.name)
      )
      val keys =
        table.sortKey.fold[jMap[String, AttributeValue]](partitionKey) { key =>
          val sortKey = new util.HashMap[String, AttributeValue]()
          sortKey.put(key.name, m.get(key.name))
          sortKey ++ partitionKey
        }
      AttributeValue.builder().m(keys).build()
    } else {
      AttributeValue.builder().build()
    }
  }

  def batchDeleteUnorderedOp[F[_]: Timer: Concurrent, P: Encoder](
    table: Table,
    maxBatchWait: FiniteDuration,
    parallelism: Int
  )(jClient: DynamoDbAsyncClient): Pipe[F, P, Unit] = { in: Stream[F, P] =>
    mkDeleteRequestOutOrdered[F, P](table, maxBatchWait).apply(in).map {
      req =>
        sendHandleLeftOver(req)(jClient)
    }.parJoin(parallelism)
  }.andThen(_.drain)

  def batchDeleteUnorderedOp[F[_]: Timer: Concurrent, P: Encoder, S: Encoder](
    table: Table,
    maxBatchWait: FiniteDuration,
    parallelism: Int
  )(jClient: DynamoDbAsyncClient): Pipe[F, (P, S), Unit] = {
    in: Stream[F, (P, S)] =>
      mkDeleteRequestOutOrdered[F, P, S](table, maxBatchWait).apply(in).map {
        req =>
          sendHandleLeftOver(req)(jClient)
      }.parJoin(parallelism)
  }.andThen(_.drain)

  def batchPutInorderedOp[F[_]: Timer: Concurrent, I: Encoder](
    table: Table,
    maxBatchWait: FiniteDuration
  )(jClient: DynamoDbAsyncClient): Pipe[F, I, Unit] = { in: Stream[F, I] =>
    mkPutRequestInOrdered[F, I](table, maxBatchWait).apply(
      in
    ).flatMap {
      req =>
        sendHandleLeftOver(req)(jClient)
    }
  }.andThen(_.drain)

  def batchPutUnorderedOp[F[_]: Timer: Concurrent, I: Encoder](
    tableName: String,
    maxBatchWait: FiniteDuration,
    parallelism: Int
  )(jClient: DynamoDbAsyncClient): Pipe[F, I, Unit] = { in: Stream[F, I] =>
    in.groupWithin(MaxBatchWriteSize, maxBatchWait).map { chunk =>
      val reqs =
        chunk.foldLeft(Map.empty[I, jMap[String, AttributeValue]]) {
          (acc, item) =>
            acc + (item -> item.asAttributeValue.m())
        }.map {
          case (_, item) =>
            val put = PutRequest.builder().item(item).build()
            WriteRequest.builder().putRequest(put).build()
        }.toList.asJava

      val writes = Map(tableName -> reqs).asJava
      BatchWriteItemRequest.builder().requestItems(writes).build()
    }.map(sendHandleLeftOver(_)(jClient)).parJoin(parallelism)
  }.andThen(_.drain)

  def batchWriteInorderedOp[F[_]: Timer: Concurrent, P: Encoder, I: Encoder](
    table: Table,
    maxBatchWait: FiniteDuration
  )(jClient: DynamoDbAsyncClient): Pipe[F, Either[P, I], Unit] = {
    in: Stream[F, Either[P, I]] =>
      mkRequestInOrdered[F, P, I](table, maxBatchWait).apply(
        in
      ).flatMap {
        req =>
          sendHandleLeftOver(req)(jClient)
      }
  }.andThen(_.drain)

  def batchWriteInorderedOp[
    F[_]: Timer: Concurrent,
    P: Encoder,
    S: Encoder,
    I: Encoder
  ](
    table: Table,
    maxBatchWait: FiniteDuration
  )(jClient: DynamoDbAsyncClient): Pipe[F, Either[(P, S), I], Unit] = {
    in: Stream[F, Either[(P, S), I]] =>
      mkRequestInOrdered[F, P, S, I](table, maxBatchWait).apply(
        in
      ).flatMap {
        req =>
          sendHandleLeftOver(req)(jClient)
      }
  }.andThen(_.drain)
}

object BatchWriteOps extends BatchWriteOps
