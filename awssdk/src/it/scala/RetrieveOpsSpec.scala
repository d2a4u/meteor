package meteor

import java.time.Instant

import cats.implicits._
import cats.effect.IO
import meteor.Util._
import meteor.codec.Encoder

class RetrieveOpsSpec extends ITSpec {

  behavior.of("retrieve operation")

  it should "return multiple items of the same partition key" in forAll {
    test: TestData =>
      val partitionKey = Id("def")
      val input =
        List(
          test.copy(id = partitionKey, range = Range("a")),
          test.copy(id = partitionKey, range = Range("b"))
        )
      tableWithKeys[IO].use[IO, List[TestData]] {
        case (client, table) =>
          val retrieval = client.retrieve[Id, TestData](
            table,
            partitionKey,
            consistentRead = false,
            Int.MaxValue
          ).compile.toList
          input.traverse(
            i => client.put[TestData](table.name, i)
          ).void >> Util.retryOf(retrieval)(
            _.size == input.length
          )
      }.unsafeToFuture().futureValue should contain theSameElementsAs input
  }

  it should "filter results by given filter expression" in forAll {
    test: List[TestData] =>
      val unique = test.map(t => (t.id, t.range) -> t).toMap.values.toList
      val partitionKey = Id(Instant.now.toString)
      val testUpdated = unique.map(t => t.copy(id = partitionKey))
      val input = testUpdated.filter(t => t.bool && t.int > 0)

      tableWithKeys[IO].use[IO, List[TestData]] {
        case (client, table) =>
          val retrieval = client.retrieve[Id, Range, TestData](
            table,
            Query[Id, Range](
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
            consistentRead = false,
            Int.MaxValue
          ).compile.toList
          testUpdated.traverse(
            i => client.put[TestData](table.name, i)
          ) >> Util.retryOf(retrieval)(_.size == input.length)
      }.unsafeToFuture().futureValue should contain theSameElementsAs input
  }

  it should "limit internal requests" in forAll {
    (test1: TestData, test2: TestData) =>
      val partitionKey = Id("def")
      val input =
        List(test1.copy(id = partitionKey), test2.copy(id = partitionKey))
      val result = tableWithKeys[IO].use[
        IO,
        List[fs2.Chunk[TestData]]
      ] {
        case (client, table) =>
          val retrieval = client.retrieve[Id, Range, TestData](
            table,
            Query[Id, Range](partitionKey, SortKeyQuery.Empty[Range]()),
            consistentRead = false,
            1
          ).chunks.compile.toList
          input.traverse(
            i => client.put[TestData](table.name, i)
          ) >> Util.retryOf(retrieval)(
            _.size == input.length
          )
      }.unsafeToFuture().futureValue
      result.forall(_.size == 1) shouldBe true
  }
}
