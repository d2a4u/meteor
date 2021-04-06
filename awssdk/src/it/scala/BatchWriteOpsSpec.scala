package meteor

import cats.implicits._
import cats.effect.IO
import meteor.Util._

import scala.concurrent.duration._

class BatchWriteOpsSpec extends ITSpec {

  behavior.of("batch write operation")

  val backOff = Client.BackoffStrategy.default

  it should "batch put items via Pipe" in {
    val size = 200
    val testData = sample[TestData]
    val input = (0 until size).map { i =>
      testData.copy(id = Id(i.toString))
    }
    val keys = fs2.Stream.emits(input).covary[IO].map { data =>
      (data.id, data.range)
    }

    compositeKeysTable[IO].use {
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
        put(fs2.Stream.emits(input).covary[IO]).compile.drain >> get(
          keys
        ).compile.toList
    }.unsafeToFuture().futureValue should contain theSameElementsAs input
  }

  it should "batch put fixed size Seq" in {
    val size = 200
    val testData = sample[TestData]
    val input = (0 until size).map { i =>
      testData.copy(id = Id(i.toString))
    }
    val keys = fs2.Stream.emits(input.map { data =>
      (data.id, data.range)
    })

    compositeKeysTable[IO].use {
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
        ).compile.toList
    }.unsafeToFuture().futureValue should contain theSameElementsAs input
  }

  it should "preserve order when batch put items" in {
    val size = 200
    val testData = sample[TestData]
    val input = fs2.Stream.range(0, size).map { i =>
      testData.copy(int = i)
    }

    compositeKeysTable[IO].use {
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

  it should "de-duplicate put, put and delete item correctly" in {
    val testData = sample[TestData]

    val input = fs2.Stream(
      testData.asRight[(Id, Range)],
      testData.copy(str = "updated").asRight[(Id, Range)],
      (testData.id, testData.range).asLeft[TestData]
    ).covary[IO]

    batchWritesAnItem(
      input,
      testData.id,
      testData.range
    ).unsafeToFuture().futureValue shouldEqual None
  }

  it should "de-duplicate put, delete and put item correctly" in {
    val testData = sample[TestData]
    val updatedTestData = testData.copy(str = "updated")

    val input = fs2.Stream(
      testData.asRight[(Id, Range)],
      (testData.id, testData.range).asLeft[TestData],
      updatedTestData.asRight[(Id, Range)]
    ).covary[IO]

    batchWritesAnItem(
      input,
      testData.id,
      testData.range
    ).unsafeToFuture().futureValue shouldEqual Some(updatedTestData)
  }

  it should "de-duplicate delete put and put item correctly" in {
    val testData = sample[TestData]
    val updatedTestData = testData.copy(str = "updated")

    val input = fs2.Stream(
      (testData.id, testData.range).asLeft[TestData],
      testData.asRight[(Id, Range)],
      updatedTestData.asRight[(Id, Range)]
    ).covary[IO]

    batchWritesAnItem(
      input,
      testData.id,
      testData.range
    ).unsafeToFuture().futureValue shouldEqual Some(updatedTestData)
  }

  it should "de-duplicate delete put and delete item correctly" in {
    val testData = sample[TestData]

    val input = fs2.Stream(
      (testData.id, testData.range).asLeft[TestData],
      testData.asRight[(Id, Range)],
      (testData.id, testData.range).asLeft[TestData]
    ).covary[IO]

    batchWritesAnItem(
      input,
      testData.id,
      testData.range
    ).unsafeToFuture().futureValue shouldEqual None
  }

  it should "batch put items unordered" in {
    val size = 200
    val testData = sample[TestData]
    val input = (0 until size).map { i =>
      testData.copy(id = Id(i.toString))
    }
    val keys = fs2.Stream.emits(input.map { data =>
      (data.id, data.range)
    })

    compositeKeysTable[IO].use {
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
        client.batchPutUnordered[TestData](
          table,
          input.toSet,
          24,
          backOff
        ) >> get(
          keys
        ).compile.toList
    }.unsafeToFuture().futureValue should contain theSameElementsAs input
  }

  private def batchWritesAnItem(
    input: fs2.Stream[IO, Either[(Id, Range), TestData]],
    itemPartitionKey: Id,
    itemSortKey: Range
  ): IO[Option[TestData]] = {
    compositeKeysTable[IO].use {
      case (client, table) =>
        val write =
          client.batchWrite[Id, Range, TestData](table, 1.second, backOff)
        val get =
          client.get[Id, Range, TestData](
            table,
            itemPartitionKey,
            itemSortKey,
            consistentRead = true
          )
        write(input).compile.drain >> get
    }
  }

}
