package meteor

import cats.effect.unsafe.IORuntime
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.concurrent.duration._

trait ITSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaFutures
    with ScalaCheckDrivenPropertyChecks {
  implicit val pc: PatienceConfig =
    PatienceConfig(scaled(10.minutes), 500.millis)

  implicit val ioRuntime: IORuntime = IORuntime.global
}
