package meteor

import cats.implicits._
import cats.effect.IO
import meteor.Util._
import org.scalacheck.Arbitrary

import scala.concurrent.duration._

class BatchWriteOpsSpec extends ITSpec {

  behavior.of("batch write operation")

  val backOff = Client.BackoffStrategy.default

  it should "batch put items via Pipe" in {
    val size = 200
    val testData = implicitly[Arbitrary[TestData]].arbitrary.sample.get
    val input = fs2.Stream.range(0, size).map { i =>
      testData.copy(id = Id(i.toString))
    }.covary[IO]
    val keys = input.map { data =>
      (data.id, data.range)
    }

    tableWithKeys[IO].use {
      case (client, table) =>
        val put =
          client.batchPut[TestData](table, 1.second, backOff)
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
        put(input).compile.drain >> get(keys).compile.drain
    }.unsafeToFuture().futureValue shouldBe an[Unit]
  }

  it should "batch put fixed size Seq" in {
    val size = 200
    val testData = implicitly[Arbitrary[TestData]].arbitrary.sample.get
    val input = (0 until size).map { i =>
      testData.copy(id = Id(i.toString))
    }
    val keys = fs2.Stream.emits(input.map { data =>
      (data.id, data.range)
    })

    tableWithKeys[IO].use {
      case (client, table) =>
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
        client.batchPut[TestData](table, input, backOff) >> get(
          keys
        ).compile.drain
    }.unsafeToFuture().futureValue shouldBe an[Unit]
  }

  it should "preserve order when batch put items" in {
    val size = 200
    val testData = implicitly[Arbitrary[TestData]].arbitrary.sample.get
    val input = fs2.Stream.range(0, size).map { i =>
      testData.copy(int = i)
    }

    tableWithKeys[IO].use {
      case (client, table) =>
        val put =
          client.batchPut[TestData](table, 1.second, backOff)
        val get =
          client.get[Id, Range, TestData](
            table,
            testData.id,
            testData.range,
            consistentRead = false
          )
        put(input).compile.drain >> get
    }.unsafeToFuture().futureValue shouldEqual Some(testData.copy(int =
      size - 1))
  }
}
