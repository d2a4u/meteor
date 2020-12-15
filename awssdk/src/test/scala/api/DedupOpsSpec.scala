package meteor
package api

import fs2.Chunk
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class DedupOpsSpec
    extends AnyFlatSpecLike
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  "deduplication" should "remove duplicated items" in forAll {
    input: List[Int] =>
      val expect = input.distinct
      val dedupped =
        DedupOps.dedupInOrdered(
          Chunk(input ++ input: _*)
        )(identity)(identity)
      dedupped.length shouldEqual expect.length
  }

  it should "not remove none duplicated items" in forAll {
    input: List[Int] =>
      val expect = input.distinct
      val dedupped =
        BatchGetOps.dedupInOrdered(Chunk(input: _*))(identity)(identity)
      dedupped should contain theSameElementsAs expect
  }

  it should "preserve ordering" in {
    val input = 0 until 100
    val dedupped =
      DedupOps.dedupInOrdered(Chunk.iterable(input))(identity)(identity)
    dedupped should contain theSameElementsInOrderAs input
  }
}
