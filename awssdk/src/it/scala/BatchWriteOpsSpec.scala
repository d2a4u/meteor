package meteor

import cats.implicits._
import cats.effect.IO
import scala.concurrent.duration._

class BatchWriteOpsSpec extends ITSpec {

  behavior.of("batch write operation")

  it should "batch write items" in forAll { tests: Seq[TestData] =>
    val tableName = Table("test_primary_keys")
    val input = fs2.Stream.emits[IO, TestData](tests)

    Client.resource[IO].use { client =>
      val put =
        client.batchPut[TestData](tableName, 1.second, 32)
      val del =
        client.batchDelete[(Id, Range)](
          tableName,
          1.second,
          32
        )
      put(input).flatMap { _ =>
        del(input.map(t => (t.id, t.range)))
      }.compile.foldMonoid
    }.unsafeToFuture().futureValue shouldBe an[Unit]
  }
}
