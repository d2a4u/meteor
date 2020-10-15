package meteor

import java.time.Instant

import cats.effect.IO
import cats.implicits._
import meteor.Util.resource
import meteor.codec.Encoder

import scala.concurrent.duration._

class RetrieveOpSpec extends ITSpec {

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
            Query(partitionKey, SortKeyQuery.Empty[Range](), Expression.empty),
            tableName,
            consistentRead = false,
            None
          ).compile.toList
          Util.retryOf[IO, List[TestData]](retrieval, 1.second, 10)(
            _.size == expect.length
          )
      }.unsafeToFuture().futureValue should contain theSameElementsAs expect
  }

  it should "filter results by given filter expression" in forAll {
    test: List[TestData] =>
      val partitionKey = Id(Instant.now.toString)
      val testUpdated = test.map(t => t.copy(id = partitionKey))
      val expect = testUpdated.filter(t => t.bool && t.int > 0)

      val src = for {
        client <- Client.resource[IO]
        r <- resource[IO, List, TestData, TestData](
          testUpdated,
          t => client.put[TestData](t, tableName).as(t),
          t => client.delete(t.id, t.range, tableName)
        )
      } yield (r, client)
      src.use[IO, List[TestData]] {
        case (_, client) =>
          val retrieval = client.retrieve[TestData, Id, Range](
            Query(
              partitionKey,
              SortKeyQuery.Empty[Range](),
              Expression(
                "#b = :bool and #i > :int",
                Map("#b" -> "bool", "#i" -> "int"),
                Map(
                  ":bool" -> Encoder[Boolean].write(true),
                  ":int" -> Encoder[Int].write(0)
                )
              )
            ),
            tableName,
            consistentRead = false,
            None
          ).compile.toList
          Util.retryOf[IO, List[TestData]](retrieval, 1.second, 10)(
            _.size == expect.length
          )
      }.unsafeToFuture().futureValue should contain theSameElementsAs expect
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
            Query(partitionKey, SortKeyQuery.Empty[Range](), Expression.empty),
            tableName,
            consistentRead = false,
            None,
            1
          ).chunks.compile.toList
          Util.retryOf(retrieval, 1.second, 10)(_.size == expect.length)
      }.unsafeToFuture().futureValue
      result.forall(_.size == 1) shouldBe true
  }
}
