package meteor

import cats.implicits._
import cats.effect.IO
import cats.effect.concurrent.Ref
import meteor.Util.{hasPrimaryKeys, localTableResource}
import org.scalacheck.Arbitrary

import scala.concurrent.duration._

class ScanOpsSpec extends ITSpec {

  behavior.of("scan operation")

  it should "return the whole table" in {
    val size = 200
    val ref = Ref.of[IO, Int](0)

    val testData = implicitly[Arbitrary[TestData]].arbitrary.sample.get
    val input = fs2.Stream.range(0, size).map { i =>
      testData.copy(id = Id(i.toString))
    }.covary[IO]

    def updated(ref: Ref[IO, Int]) =
      localTableResource[IO](hasPrimaryKeys).use {
        case (client, tableName) =>
          client.batchPut[TestData](tableName, 100.millis, 32).apply(
            input
          ).compile.drain >>
            client.scan[TestData](
              tableName,
              consistentRead = false,
              1
            ).evalMap { _ =>
              ref.update(_ + 1)
            }.compile.drain
      }

    val result =
      for {
        r <- ref
        _ <- updated(r)
        i <- r.get
      } yield i
    result.unsafeRunSync() shouldEqual size
  }
}
