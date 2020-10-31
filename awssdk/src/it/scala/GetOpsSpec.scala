package meteor

import cats.effect.IO
import meteor.Util.{
  hasPartitionKeyOnly,
  hasPrimaryKeys,
  localTableResource,
  resource
}

class GetOpsSpec extends ITSpec {

  behavior.of("get operation")

  it should "return inserted item using partition key and range key" in forAll {
    test: TestData =>
      val setup = for {
        tuple <- localTableResource[IO](hasPrimaryKeys)
        client = tuple._1
        tableName = tuple._2
        _ <- resource[IO, cats.Id, TestData, Unit](
          test,
          t => client.put[TestData](tableName, t),
          _ => client.delete(tableName, test.id, test.range)
        )
      } yield (client, tableName)
      setup.use[IO, Option[TestData]] {
        case (client, tableName) =>
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
      val setup = for {
        tuple <- localTableResource[IO](hasPartitionKeyOnly)
        client = tuple._1
        tableName = tuple._2
        _ <- resource[IO, cats.Id, TestDataSimple, Unit](
          test,
          t => client.put[TestDataSimple](tableName, t),
          _ => client.delete(tableName, test.id)
        )
      } yield (client, tableName)
      setup.use[IO, Option[TestDataSimple]] {
        case (client, tableName) =>
          client.get[TestDataSimple, Id](
            tableName,
            test.id,
            consistentRead = false
          )
      }.unsafeToFuture().futureValue shouldEqual Some(test)
  }

  it should "return None if both keys does not exist" in {
    val result = localTableResource[IO](hasPrimaryKeys).use {
      case (client, tableName) =>
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
    val result = localTableResource[IO](hasPartitionKeyOnly).use {
      case (client, tableName) =>
        client.get[TestData, Id](
          tableName,
          Id("doesnt-exists"),
          consistentRead = false
        )
    }.unsafeToFuture().futureValue
    result shouldEqual None
  }
}
