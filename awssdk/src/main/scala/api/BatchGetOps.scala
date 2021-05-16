package meteor
package api

import java.util.{Map => jMap}
import cats.effect.{Concurrent, Timer}
import cats.implicits._
import fs2.{Pipe, _}
import meteor.codec.{Decoder, Encoder}
import meteor.errors.EncoderError
import meteor.implicits._
import software.amazon.awssdk.core.retry.RetryPolicyContext
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  BatchGetItemRequest,
  BatchGetItemResponse,
  KeysAndAttributes
}

import scala.collection.immutable.Iterable
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.compat.java8.DurationConverters._

case class BatchGet(
  values: Iterable[AttributeValue],
  consistentRead: Boolean = false,
  projection: Expression = Expression.empty
)

private[meteor] trait BatchGetOps
    extends PartitionKeyBatchGetOps
    with CompositeKeysBatchGetOps {}

private[meteor] trait SharedBatchGetOps extends DedupOps {
  // 100 is the maximum amount of items for BatchGetItem
  private val MaxBatchGetSize = 100

  private[meteor] def batchGetOp[F[_]: Timer: Concurrent: RaiseThrowable](
    requests: Map[String, BatchGet],
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  )(jClient: DynamoDbAsyncClient): F[Map[String, Iterable[AttributeValue]]] = {
    val responses = requests.map {
      case (tableName, get) =>
        Stream.iterable(get.values).covary[F].chunkN(MaxBatchGetSize).mapAsync(
          parallelism
        ) {
          chunk =>
            val keysF =
              dedupInOrdered[F, AttributeValue, jMap[String, AttributeValue]](
                chunk
              ) { av =>
                if (!av.hasM) {
                  Concurrent[F].raiseError(
                    EncoderError.invalidTypeFailure(DynamoDbType.M)
                  )
                } else {
                  av.m().pure[F]
                }
              }
            keysF.map { keys =>
              val keyAndAttrs =
                mkBatchGetRequest(keys, get.consistentRead, get.projection)
              val req = Map(tableName -> keyAndAttrs).asJava
              loop[F](req, backoffStrategy)(jClient)
            }
        }.parJoin(parallelism)
    }
    Stream.iterable(responses).covary[F].flatten.compile.toList.map { resps =>
      resps.foldLeft(Map.empty[String, List[AttributeValue]]) { (acc, elem) =>
        acc ++ {
          elem.responses().asScala.map {
            case (tableName, avs) =>
              tableName -> avs.asScala.toList.map(av =>
                AttributeValue.builder().m(av).build()
              )
          }
        }
      }
    }
  }

  private[meteor] def batchGetOpInternal[
    F[_]: Timer: Concurrent: RaiseThrowable,
    K,
    T: Decoder
  ](
    tableName: String,
    consistentRead: Boolean,
    projection: Expression,
    maxBatchWait: FiniteDuration,
    jClient: DynamoDbAsyncClient,
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  )(mkKey: K => F[jMap[String, AttributeValue]]): Pipe[F, K, T] =
    in => {
      val responses =
        in.groupWithin(MaxBatchGetSize, maxBatchWait).mapAsync(parallelism) {
          chunk =>
            // remove potential duplicated keys
            dedupInOrdered[F, K, jMap[String, AttributeValue]](chunk)(
              mkKey
            ).map {
              keys =>
                val keyAndAttrs =
                  mkBatchGetRequest(keys, consistentRead, projection)
                val req = Map(tableName -> keyAndAttrs).asJava

                loop[F](req, backoffStrategy)(jClient)
            }
        }
      responses.parJoin(parallelism).flatMap(parseResponse[F, T](tableName))
    }

  private[meteor] def mkBatchGetRequest(
    keys: Seq[jMap[String, AttributeValue]],
    consistentRead: Boolean,
    projection: Expression
  ): KeysAndAttributes = {
    val bd = KeysAndAttributes.builder().consistentRead(
      consistentRead
    ).keys(keys: _*)
    if (projection.nonEmpty) {
      bd.projectionExpression(
        projection.expression
      )
      if (projection.attributeNames.isEmpty) {
        bd.build()
      } else {
        bd.expressionAttributeNames(
          projection.attributeNames.asJava
        ).build()
      }
    } else {
      bd.build()
    }
  }

  private[meteor] def parseResponse[F[_]: RaiseThrowable, U: Decoder](
    tableName: String
  )(
    resp: BatchGetItemResponse
  ): Stream[F, U] = {
    Stream.emits(resp.responses().get(tableName).asScala).covary[F].flatMap {
      av =>
        Stream.fromEither(Decoder[U].read(av)).covary[F]
    }
  }

  private[meteor] def loop[F[_]: Concurrent: Timer](
    items: jMap[
      String,
      KeysAndAttributes
    ],
    backoffStrategy: BackoffStrategy,
    retried: Int = 0
  )(
    jClient: DynamoDbAsyncClient
  ): Stream[F, BatchGetItemResponse] = {
    val req = BatchGetItemRequest.builder().requestItems(items).build()
    Stream.eval((() => jClient.batchGetItem(req)).liftF[F]).flatMap {
      resp =>
        Stream.emit(resp) ++ {
          val hasNext =
            resp.hasUnprocessedKeys && !resp.unprocessedKeys().isEmpty
          if (hasNext) {
            val nextDelay = backoffStrategy.computeDelayBeforeNextRetry(
              RetryPolicyContext.builder().retriesAttempted(retried).build()
            ).toScala
            Stream.sleep(nextDelay) >> loop[F](
              resp.unprocessedKeys(),
              backoffStrategy,
              retried + 1
            )(
              jClient
            )
          } else {
            Stream.empty
          }
        }
    }
  }
}

private[meteor] trait CompositeKeysBatchGetOps extends SharedBatchGetOps {
  private[meteor] def batchGetOp[
    F[_]: Timer: Concurrent: RaiseThrowable,
    P: Encoder,
    S: Encoder,
    T: Decoder
  ](
    table: CompositeKeysTable[P, S],
    consistentRead: Boolean,
    projection: Expression,
    maxBatchWait: FiniteDuration,
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  )(jClient: DynamoDbAsyncClient): Pipe[F, (P, S), T] = {
    in =>
      val pipe = batchGetOpInternal[F, (P, S), T](
        table.tableName,
        consistentRead,
        projection,
        maxBatchWait,
        jClient,
        parallelism,
        backoffStrategy
      ) {
        case (p, s) =>
          table.mkKey[F](p, s)
      }
      in.through(pipe)
  }

  private[meteor] def batchGetOp[
    F[_]: Concurrent: Timer,
    P: Encoder,
    S: Encoder,
    T: Decoder
  ](
    table: CompositeKeysTable[P, S],
    consistentRead: Boolean,
    projection: Expression,
    keys: Iterable[(P, S)],
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  )(jClient: DynamoDbAsyncClient): F[Iterable[T]] = {
    val pipe = batchGetOpInternal[F, (P, S), T](
      table.tableName,
      consistentRead,
      projection,
      Int.MaxValue.seconds,
      jClient,
      parallelism,
      backoffStrategy
    ) {
      case (p, s) =>
        table.mkKey[F](p, s)
    }
    Stream.iterable(keys).covary[F].through(pipe).compile.to(Iterable)
  }
}

private[meteor] trait PartitionKeyBatchGetOps extends SharedBatchGetOps {
  private[meteor] def batchGetOp[
    F[_]: Timer: Concurrent: RaiseThrowable,
    P: Encoder,
    T: Decoder
  ](
    table: PartitionKeyTable[P],
    consistentRead: Boolean,
    projection: Expression,
    maxBatchWait: FiniteDuration,
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  )(jClient: DynamoDbAsyncClient): Pipe[F, P, T] =
    batchGetOpInternal[F, P, T](
      table.tableName,
      consistentRead,
      projection,
      maxBatchWait,
      jClient,
      parallelism,
      backoffStrategy
    )(table.mkKey[F])

  private[meteor] def batchGetOp[
    F[_]: Concurrent: Timer,
    P: Encoder,
    T: Decoder
  ](
    table: PartitionKeyTable[P],
    consistentRead: Boolean,
    projection: Expression,
    keys: Iterable[P],
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  )(jClient: DynamoDbAsyncClient): F[Iterable[T]] = {
    val pipe = batchGetOpInternal[F, P, T](
      table.tableName,
      consistentRead,
      projection,
      Int.MaxValue.seconds,
      jClient,
      parallelism,
      backoffStrategy
    )(table.mkKey[F])
    Stream.iterable(keys).covary[F].through(pipe).compile.to(Iterable)
  }

}

private[meteor] object BatchGetOps extends BatchGetOps
