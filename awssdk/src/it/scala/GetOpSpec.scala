package meteor

import cats.effect.{IO, Resource}
import meteor.Util.resource

import scala.concurrent.duration._

class GetOpSpec extends ITSpec {

  behavior.of("get operation")

  it should "return inserted item using partition key and range key" in forAll {
    test: TestData =>
      val tableName = Table("test_primary_keys")
      val result = for {
        client <- Client.resource[IO]
        _ <- resource[IO, cats.Id, TestData, Unit](
          test,
          t => client.put[TestData](tableName, t).void,
          _ => client.delete(tableName, test.id, test.range)
        )
        get = client.get[TestData, Id, Range](
          tableName,
          test.id,
          test.range,
          consistentRead = false
        )
        r <- Resource.liftF(Util.retryOf[IO, Option[TestData]](
          get,
          1.second,
          10
        )(
          _.isDefined
        ))
      } yield r
      result.use[IO, Option[TestData]](
        r => IO(r)
      ).unsafeToFuture().futureValue shouldEqual Some(
        test
      )
  }

  it should "return inserted item without sort key" in forAll {
    test: TestDataSimple =>
      val tableName = Table("test_partition_key_only")
      val result = for {
        client <- Client.resource[IO]
        _ <- resource[IO, cats.Id, TestDataSimple, Unit](
          test,
          t => client.put[TestDataSimple](tableName, t).void,
          _ => client.delete(tableName, test.id)
        )
        get = client.get[TestDataSimple, Id](
          tableName,
          test.id,
          consistentRead = false
        )
        r <- Resource.liftF(Util.retryOf[IO, Option[TestDataSimple]](
          get,
          1.second,
          10
        )(
          _.isDefined
        ))
      } yield r
      result.use[IO, Option[TestDataSimple]](
        r => IO(r)
      ).unsafeToFuture().futureValue shouldEqual Some(
        test
      )
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
