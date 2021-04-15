package meteor
package api.hi

import cats.effect.IO
import cats.implicits._
import meteor.Util._
import meteor.implicits._
import meteor.errors.ConditionalCheckFailed
import software.amazon.awssdk.services.dynamodb.model.ReturnValue

class CompositeTableSpec extends ITSpec {
  behavior of "CompositeTable CRUD ops"

  val data = sample[TestData]

  it should "round trip insert and get a record" in {
    testRoundTrip(
      data,
      _.put[TestData](data)
    ).unsafeToFuture().futureValue.read shouldEqual data.some
  }

  it should "round trip conditionally insert and get a record" in {
    testRoundTrip(
      data,
      _.put[TestData](
        data,
        Expression("attribute_not_exists(id)")
      )
    ).unsafeToFuture().futureValue.read shouldEqual data.some
  }

  it should "fail inserting item that doesn't meet conditional check" in {
    def write(table: CompositeTable[IO, Id, Range]) =
      table.put[TestData](
        data,
        Expression("attribute_not_exists(id)")
      )

    testRoundTrip(
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
    def write(table: CompositeTable[IO, Id, Range], data: TestData) =
      table.put[TestData, TestData](data, Expression.empty)

    testRoundTrip(
      data,
      { table =>
        write(table, data) >> write(table, data.copy(str = "foo"))
      }
    ).unsafeToFuture().futureValue.wrote shouldEqual data.some
  }

  it should "round trip delete a record" in {
    def write(table: CompositeTable[IO, Id, Range]) =
      table.put[TestData, TestData](data, Expression.empty) >> table.delete(
        data.id,
        data.range
      )

    testRoundTrip(
      data,
      write
    ).unsafeToFuture().futureValue.read shouldEqual None
  }

  it should "round trip update a record" in {
    def write(table: CompositeTable[IO, Id, Range]) =
      table.put[TestData, TestData](data, Expression.empty) >> table.update(
        data.id,
        data.range,
        Expression(
          "SET #bool = :bool_value",
          Map("#bool" -> "bool"),
          Map(":bool_value" -> (!data.bool).asAttributeValue)
        )
      )

    testRoundTrip(
      data,
      write
    ).unsafeToFuture().futureValue.read shouldEqual data.copy(bool =
      !data.bool).some
  }

  it should "round trip update a record when a conditional expression is met" in {
    def write(table: CompositeTable[IO, Id, Range]) =
      table.put[TestData, TestData](data, Expression.empty) >> table.update(
        data.id,
        data.range,
        Expression(
          "SET #bool = :bool_value",
          Map("#bool" -> "bool"),
          Map(":bool_value" -> (!data.bool).asAttributeValue)
        ),
        Expression(
          s"attribute_exists(id)"
        )
      )

    testRoundTrip(
      data,
      write
    ).unsafeToFuture().futureValue.read shouldEqual data.copy(bool =
      !data.bool).some
  }

  it should "fail updating a record when a conditional expression is not met" in {
    def write(table: CompositeTable[IO, Id, Range]) =
      table.put[TestData, TestData](data, Expression.empty) >> table.update(
        data.id,
        data.range,
        Expression(
          "SET #bool = :bool_value",
          Map("#bool" -> "bool"),
          Map(":bool_value" -> (!data.bool).asAttributeValue)
        ),
        Expression(
          s"attribute_not_exists(id)"
        )
      )

    testRoundTrip(
      data,
      write
    ).attempt.unsafeToFuture().futureValue match {
      case Left(_: ConditionalCheckFailed) => succeed
      case _ => fail()
    }
  }

  it should "update a record and return old value" in {
    def write(table: CompositeTable[IO, Id, Range]) =
      table.put[TestData, TestData](data, Expression.empty) >> table.update[
        TestData
      ](
        data.id,
        data.range,
        ReturnValue.ALL_OLD,
        Expression(
          "SET #bool = :bool_value",
          Map("#bool" -> "bool"),
          Map(":bool_value" -> (!data.bool).asAttributeValue)
        ),
        Expression.empty
      )
    val updated = data.copy(bool = !data.bool)
    val result = testRoundTrip(data, write).unsafeToFuture().futureValue
    result.wrote shouldEqual data.some
    result.read shouldEqual updated.some
  }

  it should "update a record when a conditional expression is met and return old value" in {
    def write(table: CompositeTable[IO, Id, Range]) =
      table.put[TestData, TestData](data, Expression.empty) >> table.update[
        TestData
      ](
        data.id,
        data.range,
        ReturnValue.ALL_OLD,
        Expression(
          "SET #bool = :bool_value",
          Map("#bool" -> "bool"),
          Map(":bool_value" -> (!data.bool).asAttributeValue)
        ),
        Expression(
          s"attribute_exists(id)"
        )
      )

    val updated = data.copy(bool = !data.bool)
    val result = testRoundTrip(data, write).unsafeToFuture().futureValue
    result.wrote shouldEqual data.some
    result.read shouldEqual updated.some
  }

  it should "fail updating a record when a conditional expression is not met when return value is specified" in {
    def write(table: CompositeTable[IO, Id, Range]) =
      table.put[TestData, TestData](data, Expression.empty) >> table.update[
        TestData
      ](
        data.id,
        data.range,
        ReturnValue.ALL_OLD,
        Expression(
          "SET #bool = :bool_value",
          Map("#bool" -> "bool"),
          Map(":bool_value" -> (!data.bool).asAttributeValue)
        ),
        Expression(
          s"attribute_not_exists(id)"
        )
      )

    testRoundTrip(data, write).attempt.unsafeToFuture().futureValue match {
      case Left(_: ConditionalCheckFailed) => succeed
      case _ => fail()
    }
  }

  def testRoundTrip[T](
    data: TestData,
    write: CompositeTable[IO, Id, Range] => IO[T]
  ): IO[RoundTripResult[T]] = {
    compositeTable[IO].use { table =>
      for {
        w <- write(table)
        r <- table.get[TestData](
          data.id,
          data.range,
          consistentRead = true
        )
      } yield RoundTripResult(w, r)
    }
  }
}
