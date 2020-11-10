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

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

case class BatchGet(
  consistentRead: Boolean,
  projection: Expression,
  values: Seq[AttributeValue]
)

trait BatchGetOps {
  def batchGetOp[F[_]: Timer: Concurrent: RaiseThrowable](
    requests: Map[Table, BatchGet]
  )(jClient: DynamoDbAsyncClient): F[Map[Table, Seq[AttributeValue]]] = {
    val responses = requests.map {
      case (table, get) =>
        if (get.projection.isEmpty) {
          Stream.raiseError(InvalidExpression)
        } else {
          val grouped = get.values.grouped(100).map { avs =>
            val keyAndAttrs = {
              val keys = avs.map(_.m())
              val bd = KeysAndAttributes.builder().consistentRead(
                get.consistentRead
              ).keys(keys: _*).projectionExpression(
                get.projection.expression
              )
              if (get.projection.attributeNames.isEmpty) {
                bd.build()
              } else {
                bd.expressionAttributeNames(
                  get.projection.attributeNames.asJava
                ).build()
              }
            }
            val req = Map(table.name -> keyAndAttrs).asJava
            loop[F](req)(jClient)
          }
          Stream.fromIterator[F](grouped).parJoinUnbounded
        }
    }
    Stream.iterable(responses).covary[F].flatten.compile.toList.map { resps =>
      resps.foldLeft(Map.empty[Table, Seq[AttributeValue]]) { (acc, elem) =>
        acc ++ {
          elem.responses().asScala.map {
            case (table, avs) =>
              Table(table) -> avs.asScala.toList.map(av =>
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
    table: Table,
    consistentRead: Boolean,
    projection: Expression,
    maxBatchWait: FiniteDuration,
    parallelism: Int
  )(jClient: DynamoDbAsyncClient): Pipe[F, T, U] =
    in => {
      if (projection.isEmpty) {
        Stream.raiseError(InvalidExpression)
      } else {
        // 100 is the maximum amount of items for BatchGetItem
        val responses = in.groupWithin(100, maxBatchWait).map { chunks =>
          val keys = chunks.map(t => Encoder[T].write(t).m()).toList

          val keyAndAttrs = {
            val bd = KeysAndAttributes.builder().consistentRead(
              consistentRead
            ).keys(keys: _*).projectionExpression(
              projection.expression
            )
            if (projection.attributeNames.isEmpty) {
              bd.build()
            } else {
              bd.expressionAttributeNames(
                projection.attributeNames.asJava
              ).build()
            }
          }
          val req = Map(table.name -> keyAndAttrs).asJava

          loop[F](req)(jClient)
        }
        responses.parJoin(parallelism).flatMap { resp =>
          Stream.emits(resp.responses().get(table.name).asScala).covary[
            F
          ].flatMap {
            av =>
              Stream.fromEither(Decoder[U].read(av))
          }
        }
      }
    }

  private def loop[F[_]: Concurrent](items: jMap[String, KeysAndAttributes])(
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
