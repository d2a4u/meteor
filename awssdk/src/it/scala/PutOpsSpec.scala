package meteor

import cats.effect.{IO, Resource}
import cats.implicits._
import meteor.Util.{
  hasPartitionKeyOnly,
  hasPrimaryKeys,
  localTableResource,
  resource
}
import meteor.errors.ConditionalCheckFailed

class PutOpsSpec extends ITSpec {

  behavior.of("put operation")

  it should "success inserting item with both keys" in forAll {
    test: TestData =>
      val setup = for {
        tuple <- localTableResource[IO](hasPrimaryKeys)
        client = tuple._1
        tableName = tuple._2
        _ <- Resource.liftF(client.put[TestData](tableName, test))
      } yield tuple
      setup.use {
        case (client, tableName) =>
          client.delete(tableName, test.id, test.range)
      }.unsafeToFuture().futureValue shouldBe an[Unit]
  }

  it should "return old value after successfully inserting item with both keys" in forAll {
    old: TestData =>
      val updated = old.copy(str = old.str + "-updated")
      val setup = {
        for {
          tuple <- localTableResource[IO](hasPrimaryKeys)
          client = tuple._1
          tableName = tuple._2
          _ <- resource[IO, cats.Id, TestData, Unit](
            old,
            t => client.put[TestData](tableName, t),
            _ => client.delete(tableName, old.id, old.range)
          )
        } yield tuple
      }
      setup.use {
        case (client, tableName) =>
          client.put[TestData, TestData](tableName, updated)
      }.unsafeToFuture().futureValue shouldEqual Some(old)
  }

  it should "success inserting item without sort key" in forAll {
    test: TestDataSimple =>
      val result = localTableResource[IO](hasPartitionKeyOnly).use {
        case (client, tableName) =>
          client.put[TestDataSimple](tableName, test) >> client.delete(
            tableName,
            test.id
          )
      }
      result.unsafeToFuture().futureValue shouldBe an[Unit]
  }

  it should "return old value after successfully inserting item without sort key" in forAll {
    old: TestDataSimple =>

      val updated = old.copy(data = old.data + "-updated")
      val setup = {
        for {
          tuple <- localTableResource[IO](hasPartitionKeyOnly)
          client = tuple._1
          tableName = tuple._2
          _ <- resource[IO, cats.Id, TestDataSimple, Unit](
            old,
            t => client.put[TestDataSimple](tableName, t),
            _ => client.delete(tableName, old.id)
          )
        } yield tuple
      }
      setup.use {
        case (client, tableName) =>
          client.put[TestDataSimple, TestDataSimple](tableName, updated)
      }.unsafeToFuture().futureValue shouldEqual Some(old)
  }

  it should "success inserting item if key(s) doesn't exist by using condition expression" in forAll {
    test: TestData =>
      val setup = {
        for {
          tuple <- localTableResource[IO](hasPrimaryKeys)
          client = tuple._1
          tableName = tuple._2
          _ <- Resource.liftF(client.put[TestData](
            tableName,
            test,
            Expression(
              "attribute_not_exists(#id)",
              Map("#id" -> "id"),
              Map.empty
            )
          ))
        } yield tuple
      }
      setup.use {
        case (client, tableName) =>
          client.delete(tableName, test.id, test.range)
      }.unsafeToFuture().futureValue shouldBe an[Unit]
  }

  it should "fail inserting item if key(s) exists by using condition expression" in forAll {
    test: TestData =>
      val result = localTableResource[IO](hasPrimaryKeys).use {
        case (client, tableName) =>
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
      result.attempt.unsafeToFuture().futureValue.swap.getOrElse(
        throw new Exception("testing failure")
      ) shouldBe a[
        ConditionalCheckFailed
      ]
  }
}
