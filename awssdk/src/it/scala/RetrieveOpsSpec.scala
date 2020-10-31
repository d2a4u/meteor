package meteor

import java.time.Instant

import cats.effect.IO
import cats.implicits._
import meteor.Util.{hasPrimaryKeys, localTableResource, resource}
import meteor.codec.Encoder

class RetrieveOpsSpec extends ITSpec {

  behavior.of("retrieve operation")

  it should "return multiple items of the same partition key" in forAll {
    (test1: TestData, test2: TestData) =>
      val partitionKey = Id("def")
      val expect =
        List(test1.copy(id = partitionKey), test2.copy(id = partitionKey))
      val setup = for {
        tuple <- localTableResource[IO](hasPrimaryKeys)
        client = tuple._1
        tableName = tuple._2
        _ <- resource[IO, List, TestData, TestData](
          expect,
          t => client.put[TestData](tableName, t).as(t),
          t => client.delete(tableName, t.id, t.range)
        )
      } yield tuple
      setup.use[IO, List[TestData]] {
        case (client, tableName) =>
          val retrieval = client.retrieve[TestData, Id, Range](
            tableName,
            Query[Id, Range](
              partitionKey
            ),
            consistentRead = false,
            Int.MaxValue
          ).compile.toList
          Util.retryOf(retrieval)(_.size == expect.length)
      }.unsafeToFuture().futureValue should contain theSameElementsAs expect
  }

  it should "filter results by given filter expression" in forAll {
    test: List[TestData] =>
      val partitionKey = Id(Instant.now.toString)
      val testUpdated = test.map(t => t.copy(id = partitionKey))
      val expect = testUpdated.filter(t => t.bool && t.int > 0)

      val setup = for {
        tuple <- localTableResource[IO](hasPrimaryKeys)
        client = tuple._1
        tableName = tuple._2
        _ <- resource[IO, List, TestData, TestData](
          testUpdated,
          t => client.put[TestData](tableName, t).as(t),
          t => client.delete(tableName, t.id, t.range)
        )
      } yield tuple
      setup.use[IO, List[TestData]] {
        case (client, tableName) =>
          val retrieval = client.retrieve[TestData, Id, Range](
            tableName,
            Query[Id, Range](
              partitionKey,
              Expression(
                "#b = :bool and #i > :int",
                Map("#b" -> "bool", "#i" -> "int"),
                Map(
                  ":bool" -> Encoder[Boolean].write(true),
                  ":int" -> Encoder[Int].write(0)
                )
              )
            ),
            consistentRead = false,
            Int.MaxValue
          ).compile.toList
          Util.retryOf(retrieval)(_.size == expect.length)
      }.unsafeToFuture().futureValue should contain theSameElementsAs expect
  }

  it should "limit internal requests" in forAll {
    (test1: TestData, test2: TestData) =>
      val partitionKey = Id("def")
      val expect =
        List(test1.copy(id = partitionKey), test2.copy(id = partitionKey))
      val setup = for {
        tuple <- localTableResource[IO](hasPrimaryKeys)
        client = tuple._1
        tableName = tuple._2
        _ <- resource[IO, List, TestData, TestData](
          expect,
          t => client.put[TestData](tableName, t).as(t),
          t => client.delete(tableName, t.id, t.range)
        )
      } yield tuple
      val result = setup.use[IO, List[fs2.Chunk[TestData]]] {
        case (client, tableName) =>
          val retrieval = client.retrieve[TestData, Id, Range](
            tableName,
            Query[Id, Range](partitionKey),
            consistentRead = false,
            1
          ).chunks.compile.toList
          Util.retryOf(retrieval)(_.size == expect.length)
      }.unsafeToFuture().futureValue
      result.forall(_.size == 1) shouldBe true
  }
}
