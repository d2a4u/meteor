package meteor

import cats.effect.Async
import cats.implicits._
import meteor.errors.DecoderError

import java.util.concurrent.CompletableFuture
import java.util.{HashMap => jHashMap, Map => jMap}

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
}
