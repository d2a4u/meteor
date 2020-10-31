package meteor

import cats.effect.{ContextShift, IO, Timer}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._

trait ITSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaFutures
    with ScalaCheckDrivenPropertyChecks {
  implicit val timer: Timer[IO] = IO.timer(global)
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val pc: PatienceConfig =
    PatienceConfig(scaled(5.minutes), 500.millis)
}
