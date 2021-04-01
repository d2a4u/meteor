package meteor
package api.hi

import cats.effect.IO
import cats.implicits._
import meteor.Util._
import meteor.implicits._
import meteor.errors.ConditionalCheckFailed

class SimpleTableSpec extends ITSpec {
  behavior of "SimpleTable"

  val data = sample[TestData]

  it should "round trip insert and get a record" in {
    testWriteRoundTrip(
      data,
      _.put[TestData](data)
    ).unsafeToFuture().futureValue._2 shouldEqual data.some
  }

  it should "round trip conditionally insert and get a record" in {
    testWriteRoundTrip(
      data,
      _.put[TestData](
        data,
        Expression("attribute_not_exists(id)")
      )
    ).unsafeToFuture().futureValue._2 shouldEqual data.some
  }

  it should "fail inserting item that doesn't meet conditional check" in {
    def write(table: SimpleTable[IO, Id]) =
      table.put[TestData](
        data,
        Expression("attribute_not_exists(id)")
      )

    testWriteRoundTrip(
      data,
      { table =>
        write(table) >> write(table)
      }
    ).attempt.unsafeToFuture().futureValue match {
      case Left(_: ConditionalCheckFailed) => succeed
      case _ => fail()
    }
  }

  it should "return old item when upsert twice" in {
    def write(table: SimpleTable[IO, Id], data: TestData) =
      table.put[TestData, TestData](data, Expression.empty)

    testWriteRoundTrip(
      data,
      { table =>
        write(table, data) >> write(table, data.copy(str = "foo"))
      }
    ).unsafeToFuture().futureValue._1 shouldEqual data.some
  }

  it should "round trip delete a record" in {
    def write(table: SimpleTable[IO, Id]) =
      table.put[TestData, TestData](data, Expression.empty) >> table.delete(
        data.id
      )

    testWriteRoundTrip(
      data,
      write
    ).unsafeToFuture().futureValue._2 shouldEqual None
  }

  it should "round trip update a record" in {
    def write(table: SimpleTable[IO, Id]) =
      table.put[TestData, TestData](data, Expression.empty) >> table.update(
        data.id,
        Expression(
          "SET #bool = :bool_value",
          Map("#bool" -> "bool"),
          Map(":bool_value" -> (!data.bool).asAttributeValue)
        )
      )

    testWriteRoundTrip(
      data,
      write
    ).unsafeToFuture().futureValue._2 shouldEqual data.copy(bool =
      !data.bool).some
  }

  it should "round trip update a record when a conditional expression is met" in {
    def write(table: SimpleTable[IO, Id]) =
      table.put[TestData, TestData](data, Expression.empty) >> table.update(
        data.id,
        Expression(
          "SET #bool = :bool_value",
          Map("#bool" -> "bool"),
          Map(":bool_value" -> (!data.bool).asAttributeValue)
        ),
        Expression(
          s"attribute_exists(id)"
        )
      )

    testWriteRoundTrip(
      data,
      write
    ).unsafeToFuture().futureValue._2 shouldEqual data.copy(bool =
      !data.bool).some
  }

  it should "fail updating a record when a conditional expression is not met" in {
    def write(table: SimpleTable[IO, Id]) =
      table.put[TestData, TestData](data, Expression.empty) >> table.update(
        data.id,
        Expression(
          "SET #bool = :bool_value",
          Map("#bool" -> "bool"),
          Map(":bool_value" -> (!data.bool).asAttributeValue)
        ),
        Expression(
          s"attribute_not_exists(id)"
        )
      )

    testWriteRoundTrip(
      data,
      write
    ).attempt.unsafeToFuture().futureValue match {
      case Left(_: ConditionalCheckFailed) => succeed
      case _ => fail()
    }
  }

  def testWriteRoundTrip[T](
    data: TestData,
    write: SimpleTable[IO, Id] => IO[T]
  ): IO[(T, Option[TestData])] = {
    simpleTable[IO].use { table =>
      for {
        w <- write(table)
        r <- table.get[TestData](
          data.id,
          consistentRead = true
        )
      } yield (w, r)
    }
  }

  def testReadRoundTrip[T](
    read: SimpleTable[IO, Id] => IO[Option[T]]
  ) = {
    val data = sample[TestData]
    simpleTable[IO].use { table =>
      table.put[TestData](data) >> read(table)
    }.unsafeToFuture().futureValue shouldEqual data.some
  }
}
