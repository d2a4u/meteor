package meteor

import cats.effect.IO
import cats.implicits._
import meteor.Util._

class DeleteOpsSpec extends ITSpec {

  behavior.of("delete operation")

  it should "delete an item when using both keys" in forAll {
    test: TestData =>
      tableWithKeys[IO].use {
        case (client, table) =>
          val put = client.put[TestData](table.name, test)
          val delete = client.delete(table, test.id, test.range)
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
    test: TestDataSimple =>
      tableWithPartitionKey[IO].use {
        case (client, table) =>
          val put = client.put[TestDataSimple](table.name, test)
          val delete = client.delete(table, test.id)
          val get = client.get[Id, TestDataSimple](
            table,
            test.id,
            consistentRead = false
          )
          put >> Util.retryOf(get)(_.isDefined) >>
            delete >> Util.retryOf(get)(_.isEmpty)
      }.unsafeToFuture().futureValue shouldEqual None
  }
}
