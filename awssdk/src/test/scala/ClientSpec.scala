package meteor

import cats.effect.{ContextShift, IO, Resource, Timer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._

class ClientSpec extends AnyFlatSpec with Matchers {
  implicit val timer: Timer[IO] = IO.timer(global)
  implicit val cs: ContextShift[IO] = IO.contextShift(global)

  behavior.of("Client")

  it should "put item" in {
    val tableName = TableName("test")
    val test = TestData(Id("abc"), Range("123"), 1)
    val result = for {
      client <- Client.resource[IO]
      _ <- Util.dataResource[IO, cats.Id, TestData, Id, Range](
        test,
        _.id,
        _.range,
        tableName,
        client
      )
      get = client.get[TestData, Id, Range](
        test.id,
        test.range,
        tableName,
        consistentRead = false
      )
      r <- Resource.liftF(Util.retryOf[IO, Option[TestData]](get, 1.second, 10)(
        _.isDefined
      ))
    } yield r
    result.use[IO, Option[TestData]](
      r => IO(r)
    ).unsafeRunSync() shouldEqual Some(
      test
    )
  }

  it should "return None if key does not exist" in {
    val tableName = TableName("test")
    val result = Client.resource[IO].use { client =>
      client.get[TestData, Id, Range](
        Id("id"),
        Range("range"),
        tableName,
        consistentRead = false
      )
    }.unsafeRunSync()
    result shouldEqual None
  }

  it should "retrieve items" in {
    val tableName = TableName("test")
    val partitionKey = Id("def")
    val test1 = TestData(partitionKey, Range("123"), 1)
    val test2 = TestData(partitionKey, Range("456"), 2)
    val result = for {
      client <- Client.resource[IO]
      _ <- Util.dataResource[IO, List, TestData, Id, Range](
        List(test1, test2),
        _.id,
        _.range,
        tableName,
        client
      )
      retrieval = client.retrieve[TestData, Id, Range](
        Query(partitionKey, SortKeyQuery.Empty[Range]()),
        tableName,
        consistentRead = false
      )
      r <- Resource.liftF(Util.retryOf[IO, List[TestData]](
        retrieval,
        1.second,
        10
      )(
        _.size == 2
      ))
    } yield r
    result.use[IO, List[TestData]](
      r => IO(r)
    ).unsafeRunSync() should contain theSameElementsAs List(test1, test2)
  }
}
