package meteor
package api

import java.util.{Map => jMap}

import cats.effect.{Concurrent, Timer}
import fs2.{Pipe, _}
import meteor.codec.{Decoder, Encoder}
import meteor.errors.InvalidExpression
import meteor.implicits._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{
  BatchGetItemRequest,
  BatchGetItemResponse,
  KeysAndAttributes
}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

trait BatchGetOps {
  def batchGetOp[F[_]: Timer: Concurrent, T: Encoder, U: Decoder](
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

          def loop(items: jMap[String, KeysAndAttributes])
            : Stream[F, BatchGetItemResponse] = {
            val req = BatchGetItemRequest.builder().requestItems(items).build()
            Stream.eval((() => jClient.batchGetItem(req)).liftF[F]).flatMap {
              resp =>
                Stream.emit(resp) ++ {
                  val hasNext =
                    resp.hasUnprocessedKeys && !resp.unprocessedKeys().isEmpty
                  if (hasNext) {
                    loop(resp.unprocessedKeys())
                  } else {
                    Stream.empty
                  }
                }
            }
          }

          loop(req)
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
}
