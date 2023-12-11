package meteor

import cats.effect.Async
import cats.implicits._
import meteor.errors.DecoderError
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest

import java.util.concurrent.CompletableFuture
import java.util.{HashMap => jHashMap, Map => jMap}
import scala.jdk.CollectionConverters._

private[meteor] object implicits extends syntax {
  type FailureOr[U] = Either[DecoderError, U]

  def liftFuture[F[_], A](
    thunk: => CompletableFuture[A]
  )(implicit F: Async[F]): F[A] =
    F.fromCompletableFuture(F.delay(thunk))

  implicit class JavaMap[K, V](m1: jMap[K, V]) {
    def ++(m2: jMap[K, V]): jMap[K, V] = {
      val m3 = new jHashMap[K, V](m1)

      m2.forEach {
        case (key, value) => m3.merge(
            key,
            value,
            (_: V, r: V) => r
          )
      }
      m3
    }
  }

  implicit class RichDeleteItemRequestBuilder(val underlying: DeleteItemRequest.Builder) {
    def condition(expression: Expression): underlying.type = {
      if (expression.nonEmpty) {
        underlying.conditionExpression(expression.expression)
        if (expression.attributeValues.nonEmpty) {
          underlying.expressionAttributeValues(expression.attributeValues.asJava)
        }
        if (expression.attributeNames.nonEmpty) {
          underlying.expressionAttributeNames(expression.attributeNames.asJava)
        }
      }
      underlying
    }
  }
}
