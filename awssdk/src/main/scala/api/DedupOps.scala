package meteor
package api

import cats.MonadError
import cats.implicits._
import fs2.Chunk

private[meteor] trait DedupOps {
  private[meteor] def dedupInOrdered[F[_]: MonadError[
    *[_],
    Throwable
  ], T, U, V](
    input: Chunk[T]
  )(mkKey: T => F[U])(transform: T => F[V]): F[List[V]] = {
    val iterator = input.reverseIterator
    def dedupInternal(exists: Set[U])(soFar: List[V]): F[List[V]] = {
      if (iterator.hasNext) {
        val t = iterator.next()
        for {
          u <- mkKey(t)
          v <- transform(t)
          o <-
            if (exists.contains(u)) {
              dedupInternal(exists)(soFar)
            } else {
              dedupInternal(exists + u)(v +: soFar)
            }
        } yield o
      } else {
        soFar.pure[F]
      }
    }

    dedupInternal(Set.empty)(List.empty)
  }

  private[meteor] def dedupInOrdered[F[_]: MonadError[*[_], Throwable], T, U](
    input: Chunk[T]
  )(mkKey: T => F[U]): F[List[U]] = {
    val iterator = input.reverseIterator
    def dedupInternal(exists: Set[U])(soFar: List[U]): F[List[U]] = {
      if (iterator.hasNext) {
        val t = iterator.next()
        for {
          u <- mkKey(t)
          o <-
            if (exists.contains(u)) {
              dedupInternal(exists)(soFar)
            } else {
              dedupInternal(exists + u)(u +: soFar)
            }
        } yield o
      } else {
        soFar.pure[F]
      }
    }

    dedupInternal(Set.empty)(List.empty)
  }
}

private[meteor] object DedupOps extends DedupOps
