package meteor
package api

import java.util.{Map => jMap}
import cats.effect.{Concurrent, Timer}
import cats.implicits._
import fs2.{Pipe, _}
import meteor.codec.{Decoder, Encoder}
import meteor.errors.InvalidExpression
import meteor.implicits._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  BatchGetItemRequest,
  BatchGetItemResponse,
  KeysAndAttributes
}

import scala.annotation.tailrec
import scala.collection.immutable.Iterable
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

case class BatchGet(
  consistentRead: Boolean,
  projection: Expression,
  values: Iterable[AttributeValue]
)

trait BatchGetOps extends DedupOps {

  // 100 is the maximum amount of items for BatchGetItem
  val MaxBatchSize = 100

  def batchGetOp[F[_]: Timer: Concurrent: RaiseThrowable](
    requests: Map[String, BatchGet]
  )(jClient: DynamoDbAsyncClient): F[Map[String, Iterable[AttributeValue]]] = {
    val responses = requests.map {
      case (tableName, get) =>
        Stream.iterable(get.values).covary[F].chunkN(MaxBatchSize).map {
          chunk =>
            // remove potential duplicated keys
            val keys =
              dedupInOrdered(chunk)(identity)(_.m())
            val keyAndAttrs =
              mkRequest(keys, get.consistentRead, get.projection)
            val req = Map(tableName -> keyAndAttrs).asJava
            loop[F](req)(jClient)
        }.parJoinUnbounded
    }
    Stream.iterable(responses).covary[F].flatten.compile.toList.map { resps =>
      resps.foldLeft(Map.empty[String, Seq[AttributeValue]]) { (acc, elem) =>
        acc ++ {
          elem.responses().asScala.map {
            case (tableName, avs) =>
              tableName -> avs.asScala.toList.map(av =>
                AttributeValue.builder().m(av).build())
          }
        }
      }
    }
  }

  def batchGetOp[
    F[_]: Timer: Concurrent: RaiseThrowable,
    T: Encoder,
    U: Decoder
  ](
    tableName: String,
    consistentRead: Boolean,
    projection: Expression,
    maxBatchWait: FiniteDuration,
    parallelism: Int
  )(jClient: DynamoDbAsyncClient): Pipe[F, T, U] =
    in => {
      val responses = in.groupWithin(MaxBatchSize, maxBatchWait).map {
        chunk =>
          // remove potential duplicated keys
          val keys =
            dedupInOrdered(chunk)(
              Encoder[T].write
            )(Encoder[T].write(_).m())
          val keyAndAttrs = mkRequest(keys, consistentRead, projection)
          val req = Map(tableName -> keyAndAttrs).asJava

          loop[F](req)(jClient)
      }
      responses.parJoin(parallelism).flatMap(parseResponse[F, U](tableName))
    }

  def batchGetOp[F[_]: Concurrent, T: Encoder, U: Decoder](
    tableName: String,
    consistentRead: Boolean,
    projection: Expression,
    keys: Iterable[T]
  )(jClient: DynamoDbAsyncClient): F[Iterable[U]] =
    Stream.iterable(keys).chunkN(MaxBatchSize).flatMap { chunk =>
      val keys = dedupInOrdered(chunk)(
        Encoder[T].write
      )(Encoder[T].write(_).m())
      val keyAndAttrs = if (projection.isEmpty) {
        KeysAndAttributes.builder().consistentRead(
          consistentRead
        ).keys(keys: _*).build()
      } else {
        mkRequest(keys, consistentRead, projection)
      }
      val req = Map(tableName -> keyAndAttrs).asJava
      loop[F](req)(jClient)
    }.flatMap(parseResponse[F, U](tableName)).compile.to(Iterable)

  private[api] def mkRequest(
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

  private[api] def parseResponse[F[_]: RaiseThrowable, U: Decoder](
    tableName: String
  )(
    resp: BatchGetItemResponse
  ): Stream[F, U] = {
    Stream.emits(resp.responses().get(tableName).asScala).covary[F].flatMap {
      av =>
        Stream.fromEither(Decoder[U].read(av)).covary[F]
    }
  }

  private[api] def loop[F[_]: Concurrent](items: jMap[
    String,
    KeysAndAttributes
  ])(
    jClient: DynamoDbAsyncClient
  ): Stream[F, BatchGetItemResponse] = {
    val req = BatchGetItemRequest.builder().requestItems(items).build()
    Stream.eval((() => jClient.batchGetItem(req)).liftF[F]).flatMap {
      resp =>
        Stream.emit(resp) ++ {
          val hasNext =
            resp.hasUnprocessedKeys && !resp.unprocessedKeys().isEmpty
          if (hasNext) {
            loop[F](resp.unprocessedKeys())(jClient)
          } else {
            Stream.empty
          }
        }
    }
  }
}

object BatchGetOps extends BatchGetOps
