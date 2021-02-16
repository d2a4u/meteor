package meteor

import cats.implicits._
import cats.effect.IO
import meteor.Util._
import meteor.codec.Encoder
import meteor.errors.ConditionalCheckFailed
import software.amazon.awssdk.services.dynamodb.model.ReturnValue

class UpdateOpsSpec extends ITSpec {

  behavior.of("update operation")

  it should "return new value when condition is met" in forAll {
    data: TestData =>
      // condition is int > 0
      val newInt = 2
      val input = data.copy(int = 1)
      val expected = data.copy(int = newInt)
      val result = compositeKeysTable[IO].use {
        case (client, table) =>
          client.put[TestData](table.tableName, input) >>
            client.update[Id, Range, TestData](
              table,
              input.id,
              input.range,
              Expression(
                "SET #int_u = :int_u_value",
                Map("#int_u" -> "int"),
                Map(":int_u_value" -> Encoder[Int].write(newInt))
              ),
              Expression(
                s"#int_c > :int_c_value",
                Map("#int_c" -> "int"),
                Map(":int_c_value" -> Encoder[Int].write(0))
              ),
              ReturnValue.ALL_NEW
            )
      }
      result.unsafeToFuture().futureValue shouldEqual Some(expected)
  }

  it should "return old value when condition is met" in forAll {
    data: TestData =>
      // condition is int > 0
      val newInt = 2
      val input = data.copy(int = 1)
      val result = compositeKeysTable[IO].use {
        case (client, table) =>
          client.put[TestData](table.tableName, input) >>
            client.update[Id, Range, TestData](
              table,
              input.id,
              input.range,
              Expression(
                "SET #int_u = :int_u_value",
                Map("#int_u" -> "int"),
                Map(":int_u_value" -> Encoder[Int].write(newInt))
              ),
              Expression(
                s"#int_c > :int_c_value",
                Map("#int_c" -> "int"),
                Map(":int_c_value" -> Encoder[Int].write(0))
              ),
              ReturnValue.ALL_OLD
            )
      }
      result.unsafeToFuture().futureValue shouldEqual Some(input)
  }

  it should "raise error when condition is not met" in forAll {
    data: TestData =>
      // condition is int > 0
      val newInt = 2
      val input = data.copy(int = -1)
      val result = compositeKeysTable[IO].use {
        case (client, table) =>
          client.put[TestData](table.tableName, input) >>
            client.update[Id, Range, TestData](
              table,
              input.id,
              input.range,
              Expression(
                "SET #int_u = :int_u_value",
                Map("#int_u" -> "int"),
                Map(":int_u_value" -> Encoder[Int].write(newInt))
              ),
              Expression(
                s"#int_c > :int_c_value",
                Map("#int_c" -> "int"),
                Map(":int_c_value" -> Encoder[Int].write(0))
              ),
              ReturnValue.ALL_OLD
            )
      }
      val expect = result.attempt.unsafeToFuture().futureValue.swap.getOrElse(
        throw new Exception("testing failure")
      )
      expect shouldBe a[ConditionalCheckFailed]
  }
}
