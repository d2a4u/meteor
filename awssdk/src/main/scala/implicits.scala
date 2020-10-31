package meteor

import java.util.concurrent.{CompletableFuture, CompletionException}

import cats.effect._
import cats.implicits._
import meteor.codec.{Decoder, DecoderFailure}
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

object implicits {
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

  implicit class ToAttributeValue(m: java.util.Map[String, AttributeValue]) {
    def attemptDecode[T: Decoder]: Either[DecoderFailure, Option[T]] = {
      Option(m)
        .filter(_.size > 0)
        .map(xs => AttributeValue.builder().m(xs).build())
        .traverse[FailureOr, T](Decoder[T].read)
    }
  }
}
