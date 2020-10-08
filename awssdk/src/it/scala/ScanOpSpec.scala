package meteor

import cats.effect.concurrent.Ref
import cats.effect.{ContextShift, IO, Timer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.concurrent.ExecutionContext.global

class ScanOpSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {
  implicit val timer: Timer[IO] = IO.timer(global)
  implicit val cs: ContextShift[IO] = IO.contextShift(global)

  behavior.of("scan operation")
  val tableName = Table("test_primary_keys")

  it should "return the whole table" in {
    val tableName = Table("test_scan")
    val ref = Ref.of[IO, Int](0)
    val client = Client.resource[IO]
    def updated(ref: Ref[IO, Int]) =
      for {
        c <- fs2.Stream.resource(client)
        void <- c.scan[TestData](tableName, false, 1024).collect {
          case Some(a) => a
        }.evalMap { _ =>
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
