package meteor
package api.hi

import cats.effect.IO
import cats.implicits._
import fs2._
import meteor.Util._
import meteor.codec.Encoder

import scala.concurrent.duration.DurationInt

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

  it should "retrieve multiple items for the same partition key" in {
    val partitionKey = Id("foo")
    val data =
      List.fill(200)(sample[Range]).distinct.map { range =>
        sample[TestData].copy(
          id = partitionKey,
          range = range
        )
      }
    val result = compositeTable[IO].use { table =>
      table.batchPut[TestData](
        1.minute,
        Client.BackoffStrategy.default
      ).apply(Stream.emits(data)).compile.drain >> table.retrieve[TestData](
        partitionKey,
        consistentRead = true,
        500
      ).compile.toList
    }.unsafeToFuture().futureValue
    result.sortBy(_.range.value) should contain theSameElementsAs data.sortBy(
      _.range.value
    )
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

  it should "retrieve multiple items for the same partition key" in {
    val partitionKey = "foo"
    val data =
      List.fill(200)(sample[Range]).distinct.map { range =>
        sample[TestData].copy(
          str = partitionKey,
          range = range
        )
      }
    val result = secondaryCompositeIndex[IO].use {
      case (table, index) =>
        table.batchPut[TestData](
          1.minute,
          Client.BackoffStrategy.default
        ).apply(Stream.emits(data)).compile.drain >> index.retrieve[TestData](
          partitionKey,
          500
        ).compile.toList
    }.unsafeToFuture().futureValue
    result.sortBy(_.range.value) should contain theSameElementsAs data.sortBy(
      _.range.value
    )
  }

  "GlobalSecondarySimpleIndex" should "filter results by given filter expression" in {
    def retrieval(
      index: GlobalSecondarySimpleIndex[IO, Range],
      cond: Boolean
    ): Stream[IO, TestData] =
      index.retrieve[TestData](
        Query(
          data.range,
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
      )

    globalSecondarySimpleIndex[IO].use {
      case (table, index) =>
        val read = retrieval(index, data.bool).take(1).compile.toList
        table.put[TestData](data) >> read
    }.attempt.unsafeToFuture().futureValue match {
      case Left(err) =>
        err.printStackTrace()
        fail(err)

      case Right(list) =>
        list.head shouldEqual data
    }
  }

  it should "retrieve multiple items for the same partition key" in {
    val partitionKey = Range("foo")
    val data =
      List.fill(200)(sample[Id]).distinct.map { id =>
        sample[TestData].copy(
          id = id,
          range = partitionKey
        )
      }
    val result = globalSecondarySimpleIndex[IO].use {
      case (table, index) =>
        table.batchPut[TestData](
          1.minute,
          Client.BackoffStrategy.default
        ).apply(Stream.emits(data)).compile.drain >> index.retrieve[TestData](
          partitionKey,
          500
        ).take(200).compile.toList
    }.unsafeToFuture().futureValue
    result.sortBy(_.id.value) should contain theSameElementsAs data.sortBy(
      _.id.value
    )
  }
}
