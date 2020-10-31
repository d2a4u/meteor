package meteor

import cats.effect.IO
import meteor.Util.{hasPrimaryKeys, localTableResource}
import meteor.codec.Encoder
import meteor.errors.ConditionalCheckFailed
import software.amazon.awssdk.services.dynamodb.model.ReturnValue

class UpdateOpsSpec extends ITSpec {

  behavior.of("update operation")

  it should "return new value when condition is met" in forAll {
    test: TestData =>
      // condition is int > 0
      val newInt = 2
      val positiveTest = test.copy(int = 1)
      val expected = test.copy(int = newInt)
      val setup = for {
        tuple <- localTableResource[IO](hasPrimaryKeys)
        client = tuple._1
        tableName = tuple._2
        _ <- Util.resource[IO, cats.Id, TestData, Unit](
          positiveTest,
          t => client.put[TestData](tableName, t),
          _ => client.delete(tableName, positiveTest.id, positiveTest.range)
        )
      } yield tuple
      val result = setup.use {
        case (client, tableName) =>
          client.update[Id, Range, TestData](
            tableName,
            test.id,
            test.range,
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
    test: TestData =>
      // condition is int > 0
      val newInt = 2
      val positiveTest = test.copy(int = 1)
      val setup = for {
        tuple <- localTableResource[IO](hasPrimaryKeys)
        client = tuple._1
        tableName = tuple._2
        _ <- Util.resource[IO, cats.Id, TestData, Unit](
          positiveTest,
          t => client.put[TestData](tableName, t),
          _ => client.delete(tableName, positiveTest.id, positiveTest.range)
        )
      } yield tuple
      val result = setup.use {
        case (client, tableName) =>
          client.update[Id, Range, TestData](
            tableName,
            test.id,
            test.range,
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
      result.unsafeToFuture().futureValue shouldEqual Some(positiveTest)
  }

  it should "raise error when condition is not met" in forAll {
    test: TestData =>
      // condition is int > 0
      val newInt = 2
      val positiveTest = test.copy(int = -1)
      val setup = for {
        tuple <- localTableResource[IO](hasPrimaryKeys)
        client = tuple._1
        tableName = tuple._2
        _ <- Util.resource[IO, cats.Id, TestData, Unit](
          positiveTest,
          t => client.put[TestData](tableName, t),
          _ => client.delete(tableName, positiveTest.id, positiveTest.range)
        )
      } yield tuple
      val result = setup.use {
        case (client, tableName) =>
          client.update[Id, Range, TestData](
            tableName,
            test.id,
            test.range,
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
