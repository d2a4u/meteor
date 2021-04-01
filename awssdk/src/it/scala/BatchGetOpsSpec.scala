package meteor

import cats.effect.IO
import cats.implicits._
import meteor.Util._
import meteor.implicits._
import meteor.api.BatchGet
import meteor.codec.Encoder
import org.scalacheck.Arbitrary

import scala.concurrent.duration._

class BatchGetOpsSpec extends ITSpec {

  behavior.of("batch get operation")

  val backOff = Client.BackoffStrategy.default

  it should "batch get items from different tables" in {
    val size = 5
    val testData = sample[TestData]
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
        Map(
          "id" -> i.id.asAttributeValue,
          "range" -> i.range.asAttributeValue
        ).asAttributeValue
      }
    val valuesToGet2 =
      input2.compile.toList.map { i =>
        Map(
          "id" -> i.id.asAttributeValue
        ).asAttributeValue
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
      valuesToGet1,
      consistentRead = false,
      exp
    )
    val batchGet2 = BatchGet(
      valuesToGet2,
      consistentRead = false,
      exp
    )
    val src = for {
      src1 <- compositeKeysTable[IO]
      src2 <- partitionKeyTable[IO]
    } yield (src1, src2)

    val result = src.use {
      case ((_, table1), (client, table2)) =>
        val put1 =
          client.batchPut[TestData](table1, 1.second, backOff)
        val put2 =
          client.batchPut[TestData](table2, 1.second, backOff)
        val get =
          client.batchGet(
            Map(
              table1.tableName -> batchGet1,
              table2.tableName -> batchGet2
            ),
            50,
            backOff
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
    val testData = sample[TestData]
    val input = fs2.Stream.range(0, size).map { i =>
      testData.copy(id = Id(i.toString))
    }
    val keys = input.map { data =>
      (data.id, data.range)
    }

    compositeKeysTable[IO].use {
      case (client, table) =>
        val put =
          client.batchPut[TestData](
            table,
            1.second,
            backOff
          )
        val get =
          client.batchGet[Id, Range, TestData](
            table,
            consistentRead = false,
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
            32,
            backOff
          )
        put(input).compile.drain >> get(keys).compile.toList
    }.unsafeToFuture().futureValue should contain theSameElementsAs input.compile.toList
  }

  it should "deduplicate batch get requests" in {
    val testData = sample[TestData]
    val duplicatedKeys =
      fs2.Stream.constant((testData.id, testData.range)).take(5)

    compositeKeysTable[IO].use {
      case (client, table) =>
        val put =
          client.put[TestData](table.tableName, testData)
        val get =
          client.batchGet[Id, Range, TestData](
            table,
            consistentRead = false,
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
            32,
            backOff
          )
        put >> get(duplicatedKeys).compile.toList
    }.unsafeToFuture().futureValue should contain theSameElementsAs List(
      testData
    )
  }
}
