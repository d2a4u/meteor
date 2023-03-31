package meteor

import cats.effect.IO
import cats.implicits._
import meteor.Util._
import meteor.errors.ConditionalCheckFailed

class PutOpsSpec extends ITSpec {

  behavior.of("put operation")

  it should "success inserting item with both keys" in forAll {
    (test: TestData) =>
      compositeKeysTable[IO].use {
        case (client, table) =>
          client.put[TestData](table.tableName, test)
      }.unsafeToFuture().futureValue shouldBe an[Unit]
  }

  it should "return old value after successfully inserting item with both keys" in forAll {
    (old: TestData) =>
      val updated = old.copy(str = old.str + "-updated")
      compositeKeysTable[IO].use {
        case (client, table) =>
          client.put[TestData](table.tableName, old) >> client.put[
            TestData,
            TestData
          ](
            table.tableName,
            updated
          )
      }.unsafeToFuture().futureValue shouldEqual Some(old)
  }

  it should "return none if there isn't a previous record with the same keys" in forAll {
    (test: TestData) =>
      compositeKeysTable[IO].use {
        case (client, table) =>
          client.put[TestData, TestData](table.tableName, test)
      }.unsafeToFuture().futureValue shouldEqual None
  }

  it should "success inserting item without sort key" in forAll {
    (test: TestDataSimple) =>
      val result = partitionKeyTable[IO].use {
        case (client, table) =>
          client.put[TestDataSimple](table.tableName, test)
      }
      result.unsafeToFuture().futureValue shouldBe an[Unit]
  }

  it should "return old value after successfully inserting item without sort key" in forAll {
    (old: TestDataSimple) =>
      val updated = old.copy(data = old.data + "-updated")
      partitionKeyTable[IO].use {
        case (client, table) =>
          client.put[TestDataSimple](table.tableName, old) >> client.put[
            TestDataSimple,
            TestDataSimple
          ](table.tableName, updated)
      }.unsafeToFuture().futureValue shouldEqual Some(old)
  }

  it should "success inserting item if key(s) doesn't exist by using condition expression" in forAll {
    (test: TestData) =>
      compositeKeysTable[IO].use {
        case (client, table) =>
          client.put[TestData](
            table.tableName,
            test,
            Expression(
              "attribute_not_exists(#id)",
              Map("#id" -> "id"),
              Map.empty
            )
          )
      }.unsafeToFuture().futureValue shouldBe an[Unit]
  }

  it should "fail inserting item if key(s) exists by using condition expression" in forAll {
    (test: TestData) =>
      val result = compositeKeysTable[IO].use {
        case (client, table) =>
          client.put[TestData](
            table.tableName,
            test
          ) >>
            client.put[TestData](
              table.tableName,
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
