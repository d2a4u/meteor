package meteor

import cats.effect.{IO, Resource}
import cats.implicits._
import meteor.Util.resource
import meteor.errors.ConditionalCheckFailed

class PutOpSpec extends ITSpec {

  behavior.of("put operation")

  it should "success inserting item with both keys" in forAll {
    test: TestData =>
      val tableName = Table("test_primary_keys")
      val setup = for {
        client <- Client.resource[IO]
        _ <- Resource.liftF(client.put[TestData](tableName, test))
      } yield client
      setup.use { client =>
        client.delete(tableName, test.id, test.range)
      }.unsafeToFuture().futureValue shouldBe an[Unit]
  }

  it should "return old value after successfully inserting item with both keys" in forAll {
    old: TestData =>
      val tableName = Table("test_primary_keys")
      val updated = old.copy(str = old.str + "-updated")
      val setup = {
        for {
          client <- Client.resource[IO]
          _ <- resource[IO, cats.Id, TestData, Unit](
            old,
            t => client.put[TestData](tableName, t),
            _ => client.delete(tableName, old.id, old.range)
          )
        } yield client
      }
      setup.use { client =>
        client.put[TestData, TestData](tableName, updated)
      }.unsafeToFuture().futureValue shouldEqual Some(old)
  }

  it should "success inserting item without sort key" in forAll {
    test: TestDataSimple =>
      val tableName = Table("test_partition_key_only")
      val result = Client.resource[IO].use { client =>
        client.put[TestDataSimple](tableName, test) >> client.delete(
          tableName,
          test.id
        )
      }
      result.unsafeToFuture().futureValue shouldBe an[Unit]
  }

  it should "return old value after successfully inserting item without sort key" in forAll {
    old: TestDataSimple =>
      val tableName = Table("test_partition_key_only")
      val updated = old.copy(data = old.data + "-updated")
      val setup = {
        for {
          client <- Client.resource[IO]
          _ <- resource[IO, cats.Id, TestDataSimple, Unit](
            old,
            t => client.put[TestDataSimple](tableName, t),
            _ => client.delete(tableName, old.id)
          )
        } yield client
      }
      setup.use { client =>
        client.put[TestDataSimple, TestDataSimple](tableName, updated)
      }.unsafeToFuture().futureValue shouldEqual Some(old)
  }

  it should "success inserting item if key(s) doesn't exist by using condition expression" in forAll {
    test: TestData =>
      val tableName = Table("test_primary_keys")
      val setup = {
        for {
          client <- Client.resource[IO]
          _ <- Resource.liftF(client.put[TestData](
            tableName,
            test,
            Expression(
              "attribute_not_exists(#id)",
              Map("#id" -> "id"),
              Map.empty
            )
          ))
        } yield client
      }
      setup.use { client =>
        client.delete(tableName, test.id, test.range)
      }.unsafeToFuture().futureValue shouldBe an[Unit]
  }

  it should "fail inserting item if key(s) exists by using condition expression" in forAll {
    test: TestData =>
      val tableName = Table("test_primary_keys")
      val result = Client.resource[IO].use { client =>
        client.put[TestData](
          tableName,
          test
        ) >>
          client.put[TestData](
            tableName,
            test,
            Expression(
              "attribute_not_exists(#id)",
              Map("#id" -> "id"),
              Map.empty
            )
          )
      }
      result.attempt.unsafeToFuture().futureValue.left.get shouldBe a[
        ConditionalCheckFailed
      ]
  }
}
