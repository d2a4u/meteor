package meteor

import cats.effect.IO
import meteor.Util._
import meteor.errors.ConditionalCheckFailed

class DeleteOpsSpec extends ITSpec {

  behavior.of("delete operation")

  it should "delete an item when using both keys" in forAll {
    (test: TestData) =>
      compositeKeysTable[IO].use {
        case (client, table) =>
          val put = client.put[TestData](table.tableName, test)
          val delete = client.delete(table, test.id, test.range, Expression.empty)
          val get = client.get[Id, Range, TestData](
            table,
            test.id,
            test.range,
            consistentRead = false
          )
          put >> Util.retryOf(get)(_.isDefined) >>
            delete >> Util.retryOf(get)(_.isEmpty)
      }.unsafeToFuture().futureValue shouldEqual None
  }

  it should "delete an item when using partition key only (table doesn't have range key)" in forAll {
    (test: TestDataSimple) =>
      partitionKeyTable[IO].use {
        case (client, table) =>
          val put = client.put[TestDataSimple](table.tableName, test)
          val delete = client.delete(table, test.id, Expression.empty)
          val get = client.get[Id, TestDataSimple](
            table,
            test.id,
            consistentRead = false
          )
          put >> Util.retryOf(get)(_.isDefined) >>
            delete >> Util.retryOf(get)(_.isEmpty)
      }.unsafeToFuture().futureValue shouldEqual None
  }

  it should "success deleting item if key(s) exists by using condition expression" in forAll {
    test: TestDataSimple =>
      partitionKeyTable[IO].use {
        case (client, table) =>
          val put = client.put[TestDataSimple](table.tableName, test)
          val delete = client.delete(
            table,
            test.id,
            Expression(
              "attribute_exists(#id)",
              Map("#id" -> "id"),
              Map.empty
            )
          )
          val get = client.get[Id, TestDataSimple](
            table,
            test.id,
            consistentRead = false
          )
          put >> Util.retryOf(get)(_.isDefined) >>
            delete >> Util.retryOf(get)(_.isEmpty)
      }.unsafeToFuture().futureValue shouldBe an[Unit]
  }

  it should "fail deleting item if key(s) doesn't exist by using condition expression" in forAll {
    test: TestDataSimple =>
      val result = partitionKeyTable[IO].use {
        case (client, table) =>
          client.delete(
            table,
            test.id,
            Expression(
              "attribute_exists(#id)",
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
