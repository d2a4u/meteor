package meteor

import cats.implicits._
import cats.effect.{IO, Resource}

class DeleteOpSpec extends ITSpec {

  behavior.of("delete operation")

  it should "delete an item when using both keys" in forAll {
    test: TestData =>
      val tableName = Table("test_primary_keys")
      val result = for {
        client <- Client.resource[IO]
        put = client.put[TestData](tableName, test)
        delete = client.delete(tableName, test.id, test.range)
        get = client.get[TestData, Id, Range](
          tableName,
          test.id,
          test.range,
          consistentRead = false
        )
        pipeline =
          put >> Util.retryOf(get)(_.isDefined) >>
            delete >> Util.retryOf(get)(_.isEmpty)
        opt <- Resource.liftF(pipeline)
      } yield opt

      result.use[IO, Option[TestData]](
        r => IO(r)
      ).unsafeToFuture().futureValue shouldEqual None
  }

  it should "delete an item when using partition key only (table doesn't have range key)" in forAll {
    test: TestDataSimple =>
      val tableName = Table("test_partition_key_only")
      val result = for {
        client <- Client.resource[IO]
        put = client.put[TestDataSimple](tableName, test)
        delete = client.delete(tableName, test.id)
        get = client.get[TestDataSimple, Id](
          tableName,
          test.id,
          consistentRead = false
        )
        pipeline =
          put >> Util.retryOf(get)(_.isDefined) >>
            delete >> Util.retryOf(get)(_.isEmpty)
        opt <- Resource.liftF(pipeline)
      } yield opt

      result.use[IO, Option[TestDataSimple]](
        r => IO(r)
      ).unsafeToFuture().futureValue shouldEqual None
  }
}
