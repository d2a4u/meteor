package meteor

import cats.implicits._
import cats.effect.{ContextShift, IO, Resource, Timer}
import meteor.Util.resource
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._

class RetrieveOpSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {
  implicit val timer: Timer[IO] = IO.timer(global)
  implicit val cs: ContextShift[IO] = IO.contextShift(global)

  behavior.of("retrieve operation")
  val tableName = Table("test_primary_keys")

  it should "return multiple items of the same partition key" in forAll {
    (test1: TestData, test2: TestData) =>
      val partitionKey = Id("def")
      val expect =
        List(test1.copy(id = partitionKey), test2.copy(id = partitionKey))
      val src = for {
        client <- Client.resource[IO]
        r <- resource[IO, List, TestData, TestData](
          expect,
          t => client.put[TestData](t, tableName).as(t),
          t => client.delete(t.id, t.range, tableName)
        )
      } yield (r, client)
      src.use[IO, List[TestData]] {
        case (_, client) =>
          val retrieval = client.retrieve[TestData, Id, Range](
            Query(partitionKey, SortKeyQuery.Empty[Range]()),
            tableName,
            consistentRead = false,
            None
          ).compile.toList
          Util.retryOf[IO, List[TestData]](retrieval, 1.second, 10)(_.size == 2)
      }.unsafeRunSync() should contain theSameElementsAs expect
  }

  it should "limit internal requests" in forAll {
    (test1: TestData, test2: TestData) =>
      val partitionKey = Id("def")
      val expect =
        List(test1.copy(id = partitionKey), test2.copy(id = partitionKey))
      val src = for {
        client <- Client.resource[IO]
        r <- resource[IO, List, TestData, TestData](
          expect,
          t => client.put[TestData](t, tableName).as(t),
          t => client.delete(t.id, t.range, tableName)
        )
      } yield (r, client)
      val result = src.use[IO, List[fs2.Chunk[TestData]]] {
        case (_, client) =>
          val retrieval = client.retrieve[TestData, Id, Range](
            Query(partitionKey, SortKeyQuery.Empty[Range]()),
            tableName,
            consistentRead = false,
            None,
            1
          ).chunks.compile.toList
          Util.retryOf(retrieval, 1.second, 10)(_.size == 2)
      }.unsafeRunSync()
      result.forall(_.size == 1) shouldBe true
  }
}
