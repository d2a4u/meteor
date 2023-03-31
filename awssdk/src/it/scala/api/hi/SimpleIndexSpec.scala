package meteor
package api.hi

import cats.effect.IO
import cats.implicits._
import meteor.Util._
import meteor.codec.Encoder

class SimpleIndexSpec extends ITSpec {

  val data = sample[TestData]

  "SimpleTable" should "filter results by given filter expression" in {
    def retrieval(index: SimpleIndex[IO, Id], cond: Boolean) =
      index.retrieve[TestData](
        Query(
          data.id,
          Expression(
            "#b = :bool",
            Map("#b" -> "bool"),
            Map(
              ":bool" -> Encoder[Boolean].write(cond)
            )
          )
        ),
        consistentRead = false
      )

    simpleTable[IO].use { table =>
      val read = for {
        some <- retrieval(table, data.bool)
        none <- retrieval(table, !data.bool)
      } yield (some, none)
      table.put[TestData](data) >> read
    }.unsafeToFuture().futureValue match {
      case (Some(d), None) =>
        d shouldEqual data

      case _ =>
        fail()
    }
  }
}
