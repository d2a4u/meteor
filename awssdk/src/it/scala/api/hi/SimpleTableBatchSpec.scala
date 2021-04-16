package meteor
package api.hi

import fs2._
import cats.effect.IO
import cats.implicits._
import meteor.Util.{sample, _}

import scala.concurrent.duration.DurationInt

class SimpleTableBatchSpec extends ITSpec {
  behavior of "SimpleTable batch ops"

  val backOff = Client.BackoffStrategy.default

  it should "round trip batch put and batch get items" in {
    val samples = List.fill(200)(sample[TestData])
    val input = Stream.emits(samples).covary[IO]
    val keys = Stream.emits(samples.map(_.id)).covary[IO]

    roundTrip(input, keys, samples)
  }

  it should "deduplicate batch get items (within the same batch)" in {
    val samples = List.fill(50)(sample[TestData])
    val input = Stream.emits(samples).covary[IO]
    val keys =
      Stream.emits(samples.map(_.id) ++ samples.map(_.id)).covary[IO]

    roundTrip(input, keys, samples)
  }

  it should "preserve order batch put items" in {
    val size = 200
    val data = sample[TestData]
    val samples = List.range(0, size).map(i => data.copy(int = i))
    val input = Stream.emits(samples).covary[IO]
    val keys = Stream.emit(data.id).covary[IO]

    roundTrip(input, keys, List(data.copy(int = size - 1)))
  }

  it should "batch put items unordered" in {
    val samples = List.fill(200)(sample[TestData])
    val input = Stream.emits(samples).covary[IO]
    val keys = Stream.emits(samples.map(_.id)).covary[IO]

    roundTripUnordered(input, keys, samples)
  }

  it should "batch delete items" in {
    val samples = List.fill(200)(sample[TestData])
    val input = Stream.emits(samples).covary[IO]
    val keys = Stream.emits(samples.map(_.id)).covary[IO]

    roundTripDeletion(input, keys, List.empty)
  }

  it should "de-duplicate put, put and delete item correctly" in {
    val data = sample[TestData]

    val input = Stream(
      data.asRight[Id],
      data.copy(str = "updated").asRight[Id],
      data.id.asLeft[TestData]
    ).covary[IO]

    roundTripWrites(input, data.id, List.empty)
  }

  it should "de-duplicate put, delete and put item correctly" in {
    val data = sample[TestData]
    val updatedTestData = data.copy(str = "updated")

    val input = Stream(
      data.asRight[Id],
      data.id.asLeft[TestData],
      updatedTestData.asRight[Id]
    ).covary[IO]

    roundTripWrites(input, data.id, List(updatedTestData))
  }

  it should "de-duplicate delete put and put item correctly" in {
    val data = sample[TestData]
    val updatedTestData = data.copy(str = "updated")

    val input = Stream(
      data.id.asLeft[TestData],
      data.asRight[Id],
      updatedTestData.asRight[Id]
    ).covary[IO]

    roundTripWrites(input, data.id, List(updatedTestData))
  }

  it should "de-duplicate delete put and delete item correctly" in {
    val data = sample[TestData]

    val input = Stream(
      data.id.asLeft[TestData],
      data.asRight[Id],
      data.id.asLeft[TestData]
    ).covary[IO]

    roundTripWrites(input, data.id, List.empty)
  }

  private def put(table: SimpleTable[IO, Id]) =
    table.batchPut[TestData](
      1.second,
      backOff
    )

  private def putUnordered(table: SimpleTable[IO, Id]) =
    table.batchPutUnordered[TestData](
      1.second,
      32,
      backOff
    )

  private def delete(table: SimpleTable[IO, Id]) =
    table.batchDelete(
      1.second,
      32,
      backOff
    )

  private def write(table: SimpleTable[IO, Id]) =
    table.batchWrite[TestData](
      1.second,
      backOff
    )

  private def get(table: SimpleTable[IO, Id]) =
    table.batchGet[TestData](
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

  private def roundTrip(
    input: Stream[IO, TestData],
    keys: Stream[IO, Id],
    expect: List[TestData]
  ) = {
    simpleTable[IO].use { table =>
      put(table)(input).compile.drain >> get(table)(keys).compile.toList
    }.unsafeToFuture().futureValue should contain theSameElementsAs expect
  }

  private def roundTripDeletion(
    input: Stream[IO, TestData],
    keys: Stream[IO, Id],
    expect: List[TestData]
  ) = {
    simpleTable[IO].use { table =>
      put(table)(input).compile.drain >>
        delete(table)(keys).compile.drain >>
        get(table)(keys).compile.toList
    }.unsafeToFuture().futureValue should contain theSameElementsAs expect
  }

  private def roundTripUnordered(
    input: Stream[IO, TestData],
    keys: Stream[IO, Id],
    expect: List[TestData]
  ) = {
    simpleTable[IO].use { table =>
      putUnordered(table)(input).compile.drain >> get(table)(
        keys
      ).compile.toList
    }.unsafeToFuture().futureValue should contain theSameElementsAs expect
  }

  private def roundTripWrites(
    input: Stream[IO, Either[Id, TestData]],
    key: Id,
    expect: List[TestData]
  ) = {
    simpleTable[IO].use { table =>
      write(table)(input).compile.drain >> get(table)(
        Stream.emit(key).covary[IO]
      ).compile.toList
    }.unsafeToFuture().futureValue should contain theSameElementsAs expect
  }
}
