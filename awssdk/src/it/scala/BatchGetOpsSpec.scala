package meteor

import cats.effect.IO
import cats.implicits._
import meteor.Util._
import meteor.api.BatchGet
import meteor.codec.Encoder
import org.scalacheck.Arbitrary

import scala.concurrent.duration._

class BatchGetOpsSpec extends ITSpec {

  behavior.of("batch get operation")

  it should "batch get items from different tables" in {
    val size = 5
    val testData = implicitly[Arbitrary[TestData]].arbitrary.sample.get
    val input1 = fs2.Stream.range(0, size).map { i =>
      testData.copy(id = Id("1" + i.toString))
    }
    val input2 = fs2.Stream.range(0, size).map { i =>
      testData.copy(id = Id("2" + i.toString))
    }
    val expect1 = input1.compile.toList.map(Encoder[TestData].write)
    val expect2 = input2.compile.toList.map(Encoder[TestData].write)
    val valuesToGet1 =
      input1.compile.toList.map { i =>
        Encoder[(Id, Range)].write((i.id, i.range))
      }
    val valuesToGet2 =
      input2.compile.toList.map { i =>
        Encoder[Id].write(i.id)
      }
    val exp = Expression(
      "#id, #range, #str, #int, #bool",
      Map(
        "#id" -> "id",
        "#range" -> "range",
        "#str" -> "str",
        "#int" -> "int",
        "#bool" -> "bool"
      ),
      Map.empty
    )
    val batchGet1 = BatchGet(
      false,
      exp,
      valuesToGet1
    )
    val batchGet2 = BatchGet(
      false,
      exp,
      valuesToGet2
    )
    val src = for {
      src1 <- tableWithKeys[IO]
      src2 <- tableWithPartitionKey[IO]
    } yield (src1, src2)

    val result = src.use {
      case ((_, table1), (client, table2)) =>
        val put1 =
          client.batchPut[TestData](table1, 1.second)
        val put2 =
          client.batchPut[TestData](table2, 1.second)
        val get =
          client.batchGet(
            Map(
              table1.name -> batchGet1,
              table2.name -> batchGet2
            )
          )
        put1(input1).compile.drain >> put2(
          input2
        ).compile.drain >> get
    }.unsafeToFuture().futureValue

    result.toList.flatMap(
      _._2
    ) should contain theSameElementsAs expect1 ++ expect2
  }

  it should "batch get items" in {
    val size = 200
    val testData = implicitly[Arbitrary[TestData]].arbitrary.sample.get
    val input = fs2.Stream.range(0, size).map { i =>
      testData.copy(id = Id(i.toString))
    }
    val keys = input.map { data =>
      (data.id, data.range)
    }

    tableWithKeys[IO].use {
      case (client, table) =>
        val put =
          client.batchPut[TestData](table, 1.second)
        val get =
          client.batchGet[(Id, Range), TestData](
            table.name,
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

  it should "deduplicate batch get requests" in {
    val testData = implicitly[Arbitrary[TestData]].arbitrary.sample.get
    val duplicatedKeys =
      fs2.Stream.constant((testData.id, testData.range)).take(5)

    tableWithKeys[IO].use {
      case (client, table) =>
        val put =
          client.put[TestData](table.name, testData)
        val get =
          client.batchGet[(Id, Range), TestData](
            table.name,
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
        put >> get(duplicatedKeys).compile.toList
    }.unsafeToFuture().futureValue should contain theSameElementsAs List(
      testData
    )
  }
}
