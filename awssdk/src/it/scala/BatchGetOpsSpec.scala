package meteor

import cats.effect.IO
import cats.implicits._
import meteor.Util._
import org.scalacheck.Arbitrary

import scala.concurrent.duration._

class BatchGetOpsSpec extends ITSpec {

  behavior.of("batch get operation")

  it should "batch get items" in {
    val size = 200
    val testData = implicitly[Arbitrary[TestData]].arbitrary.sample.get
    val input = fs2.Stream.range(0, size).map { i =>
      testData.copy(id = Id(i.toString))
    }
    val keys = input.map { data =>
      (data.id, data.range)
    }

    localTableResource[IO](hasPrimaryKeys).use {
      case (client, tableName) =>
        val put =
          client.batchPut[TestData](tableName, 1.second, 32)
        val get =
          client.batchGet[(Id, Range), TestData](
            tableName,
            false,
            Expression(
              "#id, #range, #str, #int, #bool",
              Map(
                "#id" -> "id",
                "#range" -> "range",
                "#str" -> "str",
                "#int" -> "int",
                "#bool" -> "bool"
              ),
              Map.empty
            ),
            100.millis,
            32
          )
        put(input).compile.drain >> get(keys).compile.toList
    }.unsafeToFuture().futureValue should contain theSameElementsAs input.compile.toList
  }
}
