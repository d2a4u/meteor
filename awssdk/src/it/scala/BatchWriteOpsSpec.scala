package meteor

import cats.implicits._
import cats.effect.IO
import meteor.Util._
import org.scalacheck.Arbitrary

import scala.concurrent.duration._

class BatchWriteOpsSpec extends ITSpec {

  behavior.of("batch write operation")

  it should "batch write items" in {
    val size = 200
    val testData = implicitly[Arbitrary[TestData]].arbitrary.sample.get
    val input = fs2.Stream.range(0, size).map { i =>
      testData.copy(id = Id(i.toString))
    }
    val keys = input.map { data =>
      (data.id, data.range)
    }

    localTableResource[IO](hasPrimaryKeys).use {
      case (client, tableName) =>
        val put =
          client.batchPut[TestData](tableName, 1.second, 32)
        val del =
          client.batchDelete[(Id, Range)](
            tableName,
            1.second,
            32
          )
        put(input).compile.drain >> del(keys).compile.drain
    }.unsafeToFuture().futureValue shouldBe an[Unit]
  }
}
