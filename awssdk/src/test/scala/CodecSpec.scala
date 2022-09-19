package meteor

import cats._
import cats.implicits._
import meteor.Arbitraries._
import meteor.codec.{Codec, Decoder, Encoder}
import meteor.syntax._
import meteor.errors.DecoderError
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import java.time.Instant
import java.util.UUID
import scala.collection.immutable

class CodecSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {
  behavior.of("Encoder and Decoder")

  it should "successful round trip for Int" in forAll { (int: Int) =>
    roundTrip(int) shouldEqual Right(int)
  }

  it should "successful round trip for String" in forAll { (str: String) =>
    roundTrip(str) shouldEqual Right(str)
  }

  it should "successful round trip for UUID" in forAll { (uuid: UUID) =>
    roundTrip(uuid) shouldEqual Right(uuid)
  }

  it should "successful round trip for Boolean" in forAll { (bool: Boolean) =>
    roundTrip(bool) shouldEqual Right(bool)
  }

  it should "successful round trip for Long" in forAll { (long: Long) =>
    roundTrip(long) shouldEqual Right(long)
  }

  it should "successful round trip for Instant" in forAll {
    (instant: Instant) =>
      roundTrip(instant) shouldEqual Right(instant)
  }

  it should "successful round trip for Seq[String]" in forAll {
    (
      str: immutable.Seq[String]
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
      roundTripOpt(str) shouldEqual Right(str)
  }

  it should "successful round trip for Some of empty String" in {
    roundTripOpt(Some("")) shouldEqual Right(Some(""))
  }

  it should "successful round trip for Map[String, String]" in forAll {
    (
      str: Map[String, String]
    ) =>
      roundTrip(str) shouldEqual Right(str)
  }

  it should "successful round trip for Array[Byte]" in forAll {
    (bytes: Array[Byte]) =>
      roundTrip[Array[Byte]](bytes) match {
        case Right(b) => b should contain theSameElementsAs bytes
        case _        => fail()
      }
  }

  it should "successful round trip for Seq[Array[Byte]]" in forAll {
    (bytes: List[Array[Byte]]) =>
      roundTrip[immutable.List[Array[Byte]]](bytes) match {
        case Right(b)  => b should contain theSameElementsAs bytes
        case Left(err) => fail(err)
      }
  }

  def roundTrip[T: Codec](t: T): Either[DecoderError, T] = {
    Codec[T].read(
      Codec[T].write(t)
    )
  }

  def roundTripOpt[T: Decoder](t: Option[T])(implicit
  enc: Encoder[Option[T]]): Either[DecoderError, Option[T]] = {
    enc.write(t).asOpt[T]
  }
}
