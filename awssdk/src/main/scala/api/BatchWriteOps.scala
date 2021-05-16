package meteor
package api

import java.util.{Map => jMap}

import cats.implicits._
import cats.effect.{Concurrent, Timer}
import fs2.{Pipe, Stream}
import meteor.codec.Encoder
import meteor.implicits._
import software.amazon.awssdk.core.retry.RetryPolicyContext
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.compat.java8.DurationConverters._

private[meteor] trait BatchWriteOps
    extends PartitionKeyBatchWriteOps
    with CompositeKeysBatchWriteOps {}

private[meteor] trait SharedBatchWriteOps extends DedupOps {
  private[meteor] val MaxBatchWriteSize = 25

  private[meteor] def batchPutInorderedOp[F[_]: Timer: Concurrent, I: Encoder](
    table: Index[_],
    maxBatchWait: FiniteDuration,
    backoffStrategy: BackoffStrategy
  )(jClient: DynamoDbAsyncClient): Pipe[F, I, Unit] = { in: Stream[F, I] =>
    mkPutRequestInOrdered[F, I](table, maxBatchWait).apply(
      in
    ).flatMap {
      req =>
        sendHandleLeftOver(req, backoffStrategy)(jClient)
    }
  }.andThen(_.drain)

  private[meteor] def batchPutUnorderedOp[F[_]: Timer: Concurrent, I: Encoder](
    tableName: String,
    maxBatchWait: FiniteDuration,
    parallelism: Int,
    backoffStrategy: BackoffStrategy
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
    }.map(sendHandleLeftOver(_, backoffStrategy)(jClient)).parJoin(parallelism)
  }.andThen(_.drain)

  private[meteor] def sendHandleLeftOver[F[_]: Concurrent: Timer](
    req: BatchWriteItemRequest,
    backoffStrategy: BackoffStrategy,
    retried: Int = 0
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
          val nextDelay = backoffStrategy.computeDelayBeforeNextRetry(
            RetryPolicyContext.builder().retriesAttempted(retried).build()
          ).toScala
          Stream.sleep(nextDelay) >> sendHandleLeftOver(
            nextReq,
            backoffStrategy,
            retried + 1
          )(jClient)
        } else {
          Stream.empty
        }
      }
    }

  private[meteor] def mkPutRequestInOrdered[
    F[_]: Timer: Concurrent,
    I: Encoder
  ](
    table: Index[_],
    maxBatchWait: FiniteDuration
  ): Pipe[F, I, BatchWriteItemRequest] =
    _.groupWithin(MaxBatchWriteSize, maxBatchWait).evalMap { chunk =>
      def mkWriteRequest(item: I): F[WriteRequest] = {
        table.extractKey[F, I](item).as {
          val av = item.asAttributeValue.m()
          val put = PutRequest.builder().item(av).build()
          WriteRequest.builder().putRequest(put).build()
        }
      }

      def getKeys(i: I) = table.extractKey[F, I](i).map(_.asAttributeValue)
      val requestsF = dedupInOrdered[
        F,
        I,
        AttributeValue,
        WriteRequest
      ](chunk)(getKeys)(mkWriteRequest)
      requestsF.map { reqs =>
        val writes =
          Map(
            table.tableName -> reqs.asJava
          ).asJava
        BatchWriteItemRequest.builder().requestItems(
          writes
        ).build()
      }
    }
}

private[meteor] trait CompositeKeysBatchWriteOps extends SharedBatchWriteOps {
  private def mkDeleteRequestOutOrdered[
    F[_]: Timer: Concurrent,
    P: Encoder,
    S: Encoder
  ](
    table: CompositeKeysTable[P, S],
    maxBatchWait: FiniteDuration,
    parallelism: Int
  ): Pipe[F, (P, S), BatchWriteItemRequest] =
    _.groupWithin(MaxBatchWriteSize, maxBatchWait).mapAsync(parallelism) {
      chunk =>
        chunk.traverse {
          case (p, s) =>
            table.mkKey[F](p, s).map { key =>
              (p, s, key)
            }
        }.map { c =>
          val reqs = c.foldLeft(Map.empty[(P, S), WriteRequest]) {
            case (acc, (p, s, key)) =>
              val del = DeleteRequest.builder().key(key).build()
              val req = WriteRequest.builder().deleteRequest(del).build()
              acc + ((p, s) -> req)
          }.values.toList.asJava
          val writes = Map(table.tableName -> reqs).asJava
          BatchWriteItemRequest.builder().requestItems(writes).build()
        }
    }

  private def mkRequestInOrdered[
    F[_]: Timer: Concurrent,
    DP: Encoder,
    DS: Encoder,
    P: Encoder
  ](
    table: CompositeKeysTable[DP, DS],
    maxBatchWait: FiniteDuration
  ): Pipe[F, Either[(DP, DS), P], BatchWriteItemRequest] =
    _.groupWithin(MaxBatchWriteSize, maxBatchWait).evalMap { chunk =>

      def mkWriteRequest(item: Either[(DP, DS), P]): F[WriteRequest] = {
        item match {
          case Left((dp, ds)) =>
            table.mkKey[F](dp, ds).map { key =>
              val del = DeleteRequest.builder().key(key).build()
              WriteRequest.builder().deleteRequest(del).build()
            }

          case Right(p) =>
            table.extractKey[F, P](p).as {
              val put =
                PutRequest.builder().item(p.asAttributeValue.m()).build()
              WriteRequest.builder().putRequest(put).build()
            }
        }
      }

      def getKeys(item: Either[(DP, DS), P]) = {
        item match {
          case Left((dp, ds)) =>
            table.mkKey[F](dp, ds).map(_.asAttributeValue)

          case Right(p) =>
            table.extractKey[F, P](p).map(_.asAttributeValue)
        }
      }
      def requestsF =
        dedupInOrdered[
          F,
          Either[(DP, DS), P],
          AttributeValue,
          WriteRequest
        ](chunk)(getKeys)(mkWriteRequest)
      requestsF.map { requests =>
        val writes =
          Map(
            table.tableName -> requests.asJava
          ).asJava
        BatchWriteItemRequest.builder().requestItems(
          writes
        ).build()
      }
    }

  private[meteor] def batchDeleteUnorderedOp[
    F[_]: Timer: Concurrent,
    P: Encoder,
    S: Encoder
  ](
    table: CompositeKeysTable[P, S],
    maxBatchWait: FiniteDuration,
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  )(jClient: DynamoDbAsyncClient): Pipe[F, (P, S), Unit] = {
    in: Stream[F, (P, S)] =>
      mkDeleteRequestOutOrdered[F, P, S](
        table,
        maxBatchWait,
        parallelism
      ).apply(in).map {
        req =>
          sendHandleLeftOver(req, backoffStrategy)(jClient)
      }.parJoin(parallelism)
  }.andThen(_.drain)

  private[meteor] def batchWriteInorderedOp[
    F[_]: Timer: Concurrent,
    P: Encoder,
    S: Encoder,
    I: Encoder
  ](
    table: CompositeKeysTable[P, S],
    maxBatchWait: FiniteDuration,
    backoffStrategy: BackoffStrategy
  )(jClient: DynamoDbAsyncClient): Pipe[F, Either[(P, S), I], Unit] = {
    in: Stream[F, Either[(P, S), I]] =>
      mkRequestInOrdered[F, P, S, I](table, maxBatchWait).apply(
        in
      ).flatMap {
        req =>
          sendHandleLeftOver(req, backoffStrategy)(jClient)
      }
  }.andThen(_.drain)
}

private[meteor] trait PartitionKeyBatchWriteOps extends SharedBatchWriteOps {

  private def mkDeleteRequestOutOrdered[F[_]: Timer: Concurrent, P: Encoder](
    table: PartitionKeyTable[P],
    maxBatchWait: FiniteDuration
  ): Pipe[F, P, BatchWriteItemRequest] =
    _.groupWithin(MaxBatchWriteSize, maxBatchWait).evalMap { chunk =>
      chunk.traverse { p =>
        table.mkKey[F](p).map { key =>
          (p, key)
        }
      }.map { c =>
        val reqs = c.foldLeft(Map.empty[P, WriteRequest]) {
          case (acc, (p, key)) =>
            val del = DeleteRequest.builder().key(key).build()
            val req = WriteRequest.builder().deleteRequest(del).build()
            acc + (p -> req)
        }.values.toList.asJava
        val writes = Map(table.tableName -> reqs).asJava
        BatchWriteItemRequest.builder().requestItems(writes).build()
      }
    }

  private def mkRequestInOrdered[
    F[_]: Timer: Concurrent,
    DP: Encoder, // delete by partition key
    P: Encoder // put item
  ](
    table: PartitionKeyTable[DP],
    maxBatchWait: FiniteDuration
  ): Pipe[F, Either[DP, P], BatchWriteItemRequest] =
    _.groupWithin(MaxBatchWriteSize, maxBatchWait).evalMap { chunk =>
      def mkWriteRequest(item: Either[DP, P]): F[WriteRequest] = {
        item match {
          case Left(dp) =>
            table.mkKey[F](dp).map { key =>
              val del = DeleteRequest.builder().key(key).build()
              WriteRequest.builder().deleteRequest(del).build()
            }

          case Right(p) =>
            table.extractKey[F, P](p).as {
              val put =
                PutRequest.builder().item(p.asAttributeValue.m()).build()
              WriteRequest.builder().putRequest(put).build()
            }
        }
      }

      def getKeys(item: Either[DP, P]) = {
        item match {
          case Left(dp) =>
            table.mkKey[F](dp).map(_.asAttributeValue)

          case Right(p) =>
            table.extractKey[F, P](p).map(_.asAttributeValue)
        }
      }
      def requestsF =
        dedupInOrdered[F, Either[DP, P], AttributeValue, WriteRequest](chunk)(
          getKeys
        )(mkWriteRequest)
      requestsF.map { requests =>
        val writes =
          Map(
            table.tableName -> requests.asJava
          ).asJava
        BatchWriteItemRequest.builder().requestItems(
          writes
        ).build()
      }
    }

  private[meteor] def batchDeleteUnorderedOp[
    F[_]: Timer: Concurrent,
    P: Encoder
  ](
    table: PartitionKeyTable[P],
    maxBatchWait: FiniteDuration,
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  )(jClient: DynamoDbAsyncClient): Pipe[F, P, Unit] = { in: Stream[F, P] =>
    mkDeleteRequestOutOrdered[F, P](table, maxBatchWait).apply(in).map {
      req =>
        sendHandleLeftOver(req, backoffStrategy)(jClient)
    }.parJoin(parallelism)
  }.andThen(_.drain)

  private[meteor] def batchWriteInorderedOp[
    F[_]: Timer: Concurrent,
    DP: Encoder,
    P: Encoder
  ](
    table: PartitionKeyTable[DP],
    maxBatchWait: FiniteDuration,
    backoffStrategy: BackoffStrategy
  )(jClient: DynamoDbAsyncClient): Pipe[F, Either[DP, P], Unit] = {
    in: Stream[F, Either[DP, P]] =>
      mkRequestInOrdered[F, DP, P](table, maxBatchWait).apply(
        in
      ).flatMap {
        req =>
          sendHandleLeftOver(req, backoffStrategy)(jClient)
      }
  }.andThen(_.drain)
}

private[meteor] object BatchWriteOps extends BatchWriteOps
