package meteor
package api.hi

import cats.effect.IO
import cats.implicits._
import meteor.Util._
import meteor.codec.Encoder

class CompositeIndexSpec extends ITSpec {

  val data = sample[TestData]

  "CompositeIndex" should "filter results by given filter expression" in {
    def retrieval(index: CompositeIndex[IO, Id, Range], cond: Boolean) =
      index.retrieve[TestData](
        Query(
          data.id,
          SortKeyQuery.EqualTo(data.range),
          Expression(
            "#b = :bool",
            Map("#b" -> "bool"),
            Map(
              ":bool" -> Encoder[Boolean].write(cond)
            )
          )
        ),
        consistentRead = false,
        Int.MaxValue
      ).compile.toList

    compositeTable[IO].use { table =>
      val read = for {
        some <- retrieval(table, data.bool)
        none <- retrieval(table, !data.bool)
      } yield (some, none)
      table.put[TestData](data) >> read
    }.unsafeToFuture().futureValue match {
      case (s, n) if s.nonEmpty && n.isEmpty =>
        s should contain theSameElementsAs List(data)

      case _ =>
        fail()
    }
  }

  "SecondaryCompositeIndex" should "filter results by given filter expression" in {
    def retrieval(index: CompositeIndex[IO, String, Int], cond: Boolean) =
      index.retrieve[TestData](
        Query(
          data.str,
          SortKeyQuery.EqualTo(data.int),
          Expression(
            "#b = :bool",
            Map("#b" -> "bool"),
            Map(
              ":bool" -> Encoder[Boolean].write(cond)
            )
          )
        ),
        consistentRead = false,
        Int.MaxValue
      ).compile.toList

    secondaryCompositeIndex[IO].use {
      case (table, index) =>
        val read = for {
          some <- retrieval(index, data.bool)
          none <- retrieval(index, !data.bool)
        } yield (some, none)
        table.put[TestData](data) >> read
    }.unsafeToFuture().futureValue match {
      case (s, n) if s.nonEmpty && n.isEmpty =>
        s should contain theSameElementsAs List(data)

      case _ =>
        fail()
    }
  }
}
