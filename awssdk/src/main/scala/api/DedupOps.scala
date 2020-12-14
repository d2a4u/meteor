package meteor
package api

import fs2.Chunk

import scala.annotation.tailrec

trait DedupOps {
  def dedupInOrdered[T, U, V](input: Chunk[T])(mkKey: T => U)(
    transform: T => V
  ): Seq[V] = {
    val iterator = input.reverseIterator
    @tailrec
    def dedupInternal(exists: Set[U])(soFar: List[V]): List[V] = {
      if (iterator.hasNext) {
        val next = iterator.next()
        val key = mkKey(next)
        if (exists.contains(key)) {
          dedupInternal(exists)(soFar)
        } else {
          val newExists = exists ++ Set(key)
          dedupInternal(newExists)(transform(next) +: soFar)
        }
      } else {
        soFar
      }
    }
    dedupInternal(Set.empty)(List.empty)
  }
}

object DedupOps extends DedupOps
