package meteor

import cats.effect.IO
import cats.effect.concurrent.Ref

class ScanOpsSpec extends ITSpec {

  behavior.of("scan operation")
  val tableName = Table("test_primary_keys")

  it should "return the whole table" in {
    val tableName = Table("test_scan")
    val ref = Ref.of[IO, Int](0)
    val client = Client.resource[IO]
    def updated(ref: Ref[IO, Int]) =
      for {
        c <- fs2.Stream.resource(client)
        void <- c.scan[TestDataScan](
          tableName,
          consistentRead = false,
          1024
        ).evalMap { _ =>
          ref.update(_ + 1)
        }
      } yield void

    val result =
      for {
        r <- ref
        _ <- updated(r).compile.drain
        i <- r.get
      } yield i
    result.unsafeRunSync() shouldEqual 1000
  }
}
