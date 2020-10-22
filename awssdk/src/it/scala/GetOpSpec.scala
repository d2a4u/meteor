package meteor

import cats.effect.IO
import meteor.Util.resource

class GetOpSpec extends ITSpec {

  behavior.of("get operation")

  it should "return inserted item using partition key and range key" in forAll {
    test: TestData =>
      val tableName = Table("test_primary_keys")
      val setup = for {
        client <- Client.resource[IO]
        _ <- resource[IO, cats.Id, TestData, Unit](
          test,
          t => client.put[TestData](tableName, t),
          _ => client.delete(tableName, test.id, test.range)
        )
      } yield client
      setup.use[IO, Option[TestData]] { client =>
        client.get[TestData, Id, Range](
          tableName,
          test.id,
          test.range,
          consistentRead = false
        )
      }.unsafeToFuture().futureValue shouldEqual Some(test)
  }

  it should "return inserted item using partition key only (table doesn't have range key)" in forAll {
    test: TestDataSimple =>
      val tableName = Table("test_partition_key_only")
      val setup = for {
        client <- Client.resource[IO]
        _ <- resource[IO, cats.Id, TestDataSimple, Unit](
          test,
          t => client.put[TestDataSimple](tableName, t),
          _ => client.delete(tableName, test.id)
        )
      } yield client
      setup.use[IO, Option[TestDataSimple]] { client =>
        client.get[TestDataSimple, Id](
          tableName,
          test.id,
          consistentRead = false
        )
      }.unsafeToFuture().futureValue shouldEqual Some(test)
  }

  it should "return None if both keys does not exist" in {
    val tableName = Table("test_primary_keys")
    val result = Client.resource[IO].use { client =>
      client.get[TestData, Id, Range](
        tableName,
        Id("doesnt-exists"),
        Range("doesnt-exists"),
        consistentRead = false
      )
    }.unsafeToFuture().futureValue
    result shouldEqual None
  }

  it should "return None if partition key does not exist, range key is not used" in {
    val tableName = Table("test_partition_key_only")
    val result = Client.resource[IO].use { client =>
      client.get[TestData, Id](
        tableName,
        Id("doesnt-exists"),
        consistentRead = false
      )
    }.unsafeToFuture().futureValue
    result shouldEqual None
  }
}
