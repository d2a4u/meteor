package meteor

import java.util.concurrent.{CompletableFuture, CompletionException}
import java.util.{HashMap => jHashMap, Map => jMap}

import cats.effect._
import cats.implicits._
import meteor.codec.DecoderFailure

private[meteor] object implicits extends syntax {
  type FailureOr[U] = Either[DecoderFailure, U]

  implicit class FromCompletableFuture[A](thunk: () => CompletableFuture[A]) {
    def liftF[F[_]: Concurrent]: F[A] =
      Concurrent[F].cancelable[A] { cb =>

        val future = thunk().whenComplete {
          case (ok, err: CompletionException) =>
            val underlineErr = Option(err).map { e =>
              Option(e.getCause).fold[Throwable](e)(identity)
            }
            cb(underlineErr.toLeft(ok))

          case (ok, err) =>
            cb(Option(err).toLeft(ok))
        }
        Sync[F].delay(future.cancel(true)).void
      }
  }

  implicit class MergeMap[K, V](m1: jMap[K, V]) {
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
