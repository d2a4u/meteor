package meteor

import java.time.Instant
import java.util.UUID

import meteor.codec.{Decoder, DecoderFailure, Encoder}
import Arbitraries._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class CodecSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {
  behavior.of("Encoder and Decoder")

  it should "successful round trip for Int" in forAll { int: Int =>
    roundTrip(int) shouldEqual Right(int)
  }

  it should "successful round trip for String" in forAll { str: String =>
    roundTrip(str) shouldEqual Right(str)
  }

  it should "successful round trip for UUID" in forAll { uuid: UUID =>
    roundTrip(uuid) shouldEqual Right(uuid)
  }

  it should "successful round trip for Boolean" in forAll { bool: Boolean =>
    roundTrip(bool) shouldEqual Right(bool)
  }

  it should "successful round trip for Long" in forAll { long: Long =>
    roundTrip(long) shouldEqual Right(long)
  }

  it should "successful round trip for Instant" in forAll { instant: Instant =>
    roundTrip(instant) shouldEqual Right(instant)
  }

  it should "successful round trip for Seq[String]" in forAll {
    (
      str: Seq[String]
    ) =>
      roundTrip(str) shouldEqual Right(str)
  }

  it should "successful round trip for List[String]" in forAll {
    (
      str: List[String]
    ) =>
      roundTrip(str) shouldEqual Right(str)
  }

  it should "successful round trip for Option[String]" in forAll {
    (
      str: Option[String]
    ) =>
      roundTrip(str) shouldEqual Right(str)
  }

  it should "successful round trip for Map[String, String]" in forAll {
    (
      str: Map[String, String]
    ) =>
      roundTrip(str) shouldEqual Right(str)
  }

  def roundTrip[T: Decoder: Encoder](t: T): Either[DecoderFailure, T] = {
    Decoder[T].read(
      Encoder[T].write(t)
    )
  }
}
