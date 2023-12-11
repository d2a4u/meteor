package meteor
package api

import cats.implicits._
import fs2.Chunk
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.util.Try

class DedupOpsSpec
    extends AnyFlatSpecLike
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  "deduplication" should "remove duplicated items" in forAll {
    (input: List[Int]) =>
      val expect = input.distinct
      val dedupped =
        DedupOps.dedupInOrdered[Try, Int, Int, Int](
          Chunk(input ++ input: _*)
        )(Try(_))(Try(_))
      dedupped.get.length shouldEqual expect.length
  }

  it should "not remove none duplicated items" in forAll {
    (input: List[Int]) =>
      val expect = input.distinct
      val dedupped =
        BatchGetOps.dedupInOrdered[Try, Int, Int, Int](Chunk(input: _*))(
          Try(_)
        )(Try(_))
      dedupped.get should contain theSameElementsAs expect
  }

  it should "preserve ordering" in {
    val input = 0 until 100
    val dedupped =
      DedupOps.dedupInOrdered[Try, Int, Int, Int](Chunk.iterable(input))(
        Try(_)
      )(Try(_))
    dedupped.get should contain theSameElementsInOrderAs input
  }
}
