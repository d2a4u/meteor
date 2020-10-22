package meteor

import cats.implicits._
import cats.effect.{IO, Resource}

class DeleteOpSpec extends ITSpec {

  behavior.of("delete operation")

  it should "delete an item when using both keys" in forAll {
    test: TestData =>
      val tableName = Table("test_primary_keys")
      Client.resource[IO].use { client =>
        val put = client.put[TestData](tableName, test)
        val delete = client.delete(tableName, test.id, test.range)
        val get = client.get[TestData, Id, Range](
          tableName,
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
      val tableName = Table("test_partition_key_only")
      Client.resource[IO].use { client =>
        val put = client.put[TestDataSimple](tableName, test)
        val delete = client.delete(tableName, test.id)
        val get = client.get[TestDataSimple, Id](
          tableName,
          test.id,
          consistentRead = false
        )
        put >> Util.retryOf(get)(_.isDefined) >>
          delete >> Util.retryOf(get)(_.isEmpty)
      }.unsafeToFuture().futureValue shouldEqual None
  }
}
