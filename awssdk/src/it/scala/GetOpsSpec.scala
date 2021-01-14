package meteor

import cats.implicits._
import cats.effect.IO
import meteor.Util._

class GetOpsSpec extends ITSpec {

  behavior.of("get operation")

  it should "return inserted item using partition key and range key" in forAll {
    test: TestData =>
      tableWithKeys[IO].use[IO, Option[TestData]] {
        case (client, table) =>
          client.put[TestData](table.name, test) >>
            client.get[Id, Range, TestData](
              table,
              test.id,
              test.range,
              consistentRead = false
            )
      }.unsafeToFuture().futureValue shouldEqual Some(test)
  }

  it should "return inserted item using partition key only (table doesn't have range key)" in forAll {
    test: TestDataSimple =>
      tableWithPartitionKey[IO].use[
        IO,
        Option[TestDataSimple]
      ] {
        case (client, table) =>
          client.put[TestDataSimple](table.name, test) >>
            client.get[Id, TestDataSimple](
              table,
              test.id,
              consistentRead = false
            )
      }.unsafeToFuture().futureValue shouldEqual Some(test)
  }

  it should "return None if both keys does not exist" in {
    val result = tableWithKeys[IO].use {
      case (client, table) =>
        client.get[Id, Range, TestData](
          table,
          Id("doesnt-exists"),
          Range("doesnt-exists"),
          consistentRead = false
        )
    }.unsafeToFuture().futureValue
    result shouldEqual None
  }

  it should "return None if partition key does not exist, range key is not used" in {
    val result = tableWithPartitionKey[IO].use {
      case (client, table) =>
        client.get[Id, TestData](
          table,
          Id("doesnt-exists"),
          consistentRead = false
        )
    }.unsafeToFuture().futureValue
    result shouldEqual None
  }
}
