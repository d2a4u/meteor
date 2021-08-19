package meteor

import fs2._
import java.time.Instant
import cats.implicits._
import cats.effect.IO
import meteor.Util._
import meteor.codec.Encoder

import scala.concurrent.duration.DurationInt

class RetrieveOpsSpec extends ITSpec {

  behavior.of("retrieve operation")

  it should "return no items if the partition key doesn't exist" in forAll {
    test: TestData =>
      val partitionKey = Id("def")
      val input =
        List(
          test.copy(id = partitionKey, range = Range("a")),
          test.copy(id = partitionKey, range = Range("b"))
        )
      val result = compositeKeysTable[IO].use[List[TestData]] {
        case (client, table) =>
          val retrieval = client.retrieve[Id, TestData](
            table,
            Id("doesntexist"),
            consistentRead = false,
            Int.MaxValue
          ).compile.toList
          input.traverse(i =>
            client.put[TestData](table.tableName, i)
          ).void >> retrieval
      }.unsafeToFuture().futureValue
      result shouldBe empty
  }

  it should "return multiple items of the same partition key" in forAll {
    test: TestData =>
      val partitionKey = Id("def")
      val input =
        List(
          test.copy(id = partitionKey, range = Range("a")),
          test.copy(id = partitionKey, range = Range("b"))
        )
      compositeKeysTable[IO].use[List[TestData]] {
        case (client, table) =>
          val retrieval = client.retrieve[Id, TestData](
            table,
            partitionKey,
            consistentRead = false,
            Int.MaxValue
          ).compile.toList
          input.traverse(i =>
            client.put[TestData](table.tableName, i)
          ).void >> Util.retryOf(retrieval)(
            _.size == input.length
          )
      }.unsafeToFuture().futureValue should contain theSameElementsAs input
  }

  it should "return large number of items of the same partition key" in {
    val numberOfItems = 10000
    val test = sample[TestData]
    val partitionKey = Id("def")
    val input = Stream.range(1, numberOfItems).map { i =>
      test.copy(id = partitionKey, range = Range(i.toString))
    }.covary[IO]

    compositeKeysTable[IO].use[List[TestData]] {
      case (client, table) =>
        val retrieval = client.retrieve[Id, TestData](
          table,
          partitionKey,
          consistentRead = false,
          Int.MaxValue
        ).compile.toList
        client.batchPut[TestData](
          table,
          100.millis,
          Client.BackoffStrategy.default
        ).apply(input).compile.drain >> retrieval
    }.unsafeToFuture().futureValue.length shouldBe numberOfItems - 1
  }

  it should "exact item by EqualTo key expression" in forAll {
    test: TestData =>
      val result = compositeKeysTable[IO].use[TestData] {
        case (client, table) =>
          val retrieval = client.retrieve[Id, Range, TestData](
            table,
            Query[Id, Range](
              test.id,
              SortKeyQuery.EqualTo(test.range)
            ),
            consistentRead = false,
            Int.MaxValue
          ).compile.lastOrError
          client.put[TestData](table.tableName, test) >> retrieval
      }.unsafeToFuture().futureValue
      result shouldEqual test
  }

  it should "query secondary index" in forAll {
    test: TestData =>
      val input = test.copy(str = "test", int = 0)
      val result =
        compositeKeysWithSecondaryIndexTable[IO].use {
          case (client, table, secondaryIndex) =>
            val retrieval = client.retrieve[String, Int, TestData](
              secondaryIndex,
              Query[String, Int](
                input.str,
                SortKeyQuery.EqualTo(input.int)
              ),
              consistentRead = false,
              Int.MaxValue
            ).compile.lastOrError
            client.put[TestData](table.tableName, input) >> retrieval
        }.unsafeToFuture().futureValue
      result shouldEqual input
  }

  it should "filter results by given filter expression for PartitionKeyTable" in forAll {
    test: TestData =>

      partitionKeyTable[IO].use {
        case (client, table) =>
          def retrieval(cond: Boolean) =
            client.retrieve[Id, TestData](
              table,
              Query(
                test.id,
                Expression(
                  "#b = :bool",
                  Map("#b" -> "bool"),
                  Map(
                    ":bool" -> Encoder[Boolean].write(cond)
                  )
                )
              ),
              consistentRead = false
            )
          client.put[TestData](table.tableName, test) >> Util.retryOf(
            for {
              some <- retrieval(test.bool)
              none <- retrieval(!test.bool)
            } yield (some, none)
          ) {
            case (some, none) =>
              some.isDefined && none.isEmpty
          }
      }.unsafeToFuture().futureValue shouldEqual (Some(test), None)
  }

  it should "filter results by given filter expression for CompositeKeysTable" in forAll {
    test: List[TestData] =>
      val unique = test.map(t => (t.id, t.range) -> t).toMap.values.toList
      val partitionKey = Id(Instant.now.toString)
      val testUpdated = unique.map(t => t.copy(id = partitionKey))
      val input = testUpdated.filter(t => t.bool && t.int > 0)

      compositeKeysTable[IO].use[List[TestData]] {
        case (client, table) =>
          val retrieval = client.retrieve[Id, Range, TestData](
            table,
            Query[Id, Range](
              partitionKey,
              SortKeyQuery.empty[Range],
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
          testUpdated.traverse(i =>
            client.put[TestData](table.tableName, i)
          ) >> Util.retryOf(retrieval)(_.size == input.length)
      }.unsafeToFuture().futureValue should contain theSameElementsAs input
  }

  it should "limit internal requests" in forAll {
    (test1: TestData, test2: TestData) =>
      val partitionKey = Id("def")
      val input =
        List(test1.copy(id = partitionKey), test2.copy(id = partitionKey))
      val result = compositeKeysTable[IO].use[List[fs2.Chunk[TestData]]] {
        case (client, table) =>
          val retrieval = client.retrieve[Id, Range, TestData](
            table,
            Query[Id, Range](partitionKey, SortKeyQuery.empty[Range]),
            consistentRead = false,
            1
          ).chunks.compile.toList
          input.traverse(i =>
            client.put[TestData](table.tableName, i)
          ) >> Util.retryOf(retrieval)(
            _.size == input.length
          )
      }.unsafeToFuture().futureValue
      result.forall(_.size == 1) shouldBe true
  }
}
