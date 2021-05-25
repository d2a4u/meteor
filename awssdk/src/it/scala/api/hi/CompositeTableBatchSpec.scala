package meteor
package api.hi

import fs2._
import cats.effect.IO
import cats.implicits._
import meteor.Util.{sample, _}

import scala.concurrent.duration.DurationInt

class CompositeTableBatchSpec extends ITSpec {
  behavior of "CompositeTable batch ops"

  val backOff = Client.BackoffStrategy.default

  it should "round trip batch put and batch get items" in {
    val samples = Set.fill(200)(sample[Range]).toList.map { range =>
      sample[TestData].copy(range = range)
    }
    val input = Stream.emits(samples).covary[IO]
    val keys = Stream.emits(samples.map(i => (i.id, i.range))).covary[IO]

    roundTrip(input, keys, samples)
  }

  it should "deduplicate batch get items (within the same batch)" in {
    val samples = Set.fill(50)(sample[Range]).toList.map { range =>
      sample[TestData].copy(range = range)
    }
    val input = Stream.emits(samples).covary[IO]
    val keys =
      Stream.emits(samples.map(i => (i.id, i.range)) ++ samples.map(i =>
        (i.id, i.range))).covary[IO]

    roundTrip(input, keys, samples)
  }

  it should "preserve order batch put items" in {
    val size = 200
    val data = sample[TestData]
    val samples = List.range(0, size).map(i => data.copy(int = i))
    val input = Stream.emits(samples).covary[IO]
    val keys = Stream.emit((data.id, data.range)).covary[IO]

    roundTrip(input, keys, List(data.copy(int = size - 1)))
  }

  it should "batch put items unordered" in {
    val samples = Set.fill(200)(sample[Range]).toList.map { range =>
      sample[TestData].copy(range = range)
    }
    val input = Stream.emits(samples).covary[IO]
    val keys = Stream.emits(samples.map(i => (i.id, i.range))).covary[IO]

    roundTripUnordered(input, keys, samples)
  }

  it should "batch delete items" in {
    val samples = Set.fill(200)(sample[Range]).toList.map { range =>
      sample[TestData].copy(range = range)
    }
    val input = Stream.emits(samples).covary[IO]
    val keys = Stream.emits(samples.map(i => (i.id, i.range))).covary[IO]

    roundTripDeletion(input, keys, List.empty)
  }

  it should "de-duplicate put, put and delete item correctly" in {
    val data = sample[TestData]

    val input = Stream(
      data.asRight[(Id, Range)],
      data.copy(str = "updated").asRight[(Id, Range)],
      (data.id, data.range).asLeft[TestData]
    ).covary[IO]

    roundTripWrites(input, (data.id, data.range), List.empty)
  }

  it should "de-duplicate put, delete and put item correctly" in {
    val data = sample[TestData]
    val updatedTestData = data.copy(str = "updated")

    val input = Stream(
      data.asRight[(Id, Range)],
      (data.id, data.range).asLeft[TestData],
      updatedTestData.asRight[(Id, Range)]
    ).covary[IO]

    roundTripWrites(input, (data.id, data.range), List(updatedTestData))
  }

  it should "de-duplicate delete put and put item correctly" in {
    val data = sample[TestData]
    val updatedTestData = data.copy(str = "updated")

    val input = Stream(
      (data.id, data.range).asLeft[TestData],
      data.asRight[(Id, Range)],
      updatedTestData.asRight[(Id, Range)]
    ).covary[IO]

    roundTripWrites(input, (data.id, data.range), List(updatedTestData))
  }

  it should "de-duplicate delete put and delete item correctly" in {
    val data = sample[TestData]

    val input = Stream(
      (data.id, data.range).asLeft[TestData],
      data.asRight[(Id, Range)],
      (data.id, data.range).asLeft[TestData]
    ).covary[IO]

    roundTripWrites(input, (data.id, data.range), List.empty)
  }

  private def put(table: CompositeTable[IO, Id, Range]) =
    table.batchPut[TestData](
      1.second,
      backOff
    )

  private def putUnordered(table: CompositeTable[IO, Id, Range]) =
    table.batchPutUnordered[TestData](
      1.second,
      32,
      backOff
    )

  private def delete(table: CompositeTable[IO, Id, Range]) =
    table.batchDelete(
      1.second,
      32,
      backOff
    )

  private def write(table: CompositeTable[IO, Id, Range]) =
    table.batchWrite[TestData](
      1.second,
      backOff
    )

  private def get(table: CompositeTable[IO, Id, Range]) =
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
    keys: Stream[IO, (Id, Range)],
    expect: List[TestData]
  ) = {
    compositeTable[IO].use { table =>
      put(table)(input).compile.drain >> get(table)(keys).compile.toList
    }.unsafeToFuture().futureValue should contain theSameElementsAs expect
  }

  private def roundTripDeletion(
    input: Stream[IO, TestData],
    keys: Stream[IO, (Id, Range)],
    expect: List[TestData]
  ) = {
    compositeTable[IO].use { table =>
      put(table)(input).compile.drain >>
        delete(table)(keys).compile.drain >>
        get(table)(keys).compile.toList
    }.unsafeToFuture().futureValue should contain theSameElementsAs expect
  }

  private def roundTripUnordered(
    input: Stream[IO, TestData],
    keys: Stream[IO, (Id, Range)],
    expect: List[TestData]
  ) = {
    compositeTable[IO].use { table =>
      putUnordered(table)(input).compile.drain >> get(table)(
        keys
      ).compile.toList
    }.unsafeToFuture().futureValue should contain theSameElementsAs expect
  }

  private def roundTripWrites(
    input: Stream[IO, Either[(Id, Range), TestData]],
    key: (Id, Range),
    expect: List[TestData]
  ) = {
    compositeTable[IO].use { table =>
      write(table)(input).compile.drain >> get(table)(
        Stream.emit(key).covary[IO]
      ).compile.toList
    }.unsafeToFuture().futureValue should contain theSameElementsAs expect
  }
}
