package meteor

import java.time.Instant
import java.util.UUID

import meteor.codec.{Decoder, Encoder}
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
    Decoder[Int].read(Encoder[Int].write(int)) shouldEqual Right(int)
  }

  it should "successful round trip for String" in forAll { str: String =>
    Decoder[String].read(Encoder[String].write(str)) shouldEqual Right(str)
  }

  it should "successful round trip for UUID" in forAll { uuid: UUID =>
    Decoder[UUID].read(Encoder[UUID].write(uuid)) shouldEqual Right(uuid)
  }

  it should "successful round trip for Boolean" in forAll { bool: Boolean =>
    Decoder[Boolean].read(Encoder[Boolean].write(bool)) shouldEqual Right(bool)
  }

  it should "successful round trip for Long" in forAll { long: Long =>
    Decoder[Long].read(Encoder[Long].write(long)) shouldEqual Right(long)
  }

  it should "successful round trip for Instant" in forAll { instant: Instant =>
    Decoder[Instant].read(Encoder[Instant].write(instant)) shouldEqual Right(
      instant
    )
  }

  it should "successful round trip for Option[T]" in forAll {
    (
      int: Option[Int],
      str: Option[String],
      uuid: Option[UUID],
      bool: Option[Boolean],
      long: Option[Long],
      instant: Option[Instant]
    ) =>
      Decoder[Option[Int]].read(
        Encoder[Option[Int]].write(int)
      ) shouldEqual Right(int)
      Decoder[Option[String]].read(
        Encoder[Option[String]].write(str)
      ) shouldEqual Right(str)
      Decoder[Option[UUID]].read(
        Encoder[Option[UUID]].write(uuid)
      ) shouldEqual Right(uuid)
      Decoder[Option[Boolean]].read(
        Encoder[Option[Boolean]].write(bool)
      ) shouldEqual Right(bool)
      Decoder[Option[Long]].read(
        Encoder[Option[Long]].write(long)
      ) shouldEqual Right(long)
      Decoder[Option[Instant]].read(
        Encoder[Option[Instant]].write(instant)
      ) shouldEqual Right(instant)
  }

  it should "successful round trip for Map[String, T]" in forAll {
    (
      int: Map[String, Int],
      str: Map[String, String],
      uuid: Map[String, UUID],
      bool: Map[String, Boolean],
      long: Map[String, Long],
      instant: Map[String, Instant]
    ) =>
      Decoder[Map[String, Int]].read(
        Encoder[Map[String, Int]].write(int)
      ) shouldEqual Right(int)
      Decoder[Map[String, String]].read(
        Encoder[Map[String, String]].write(str)
      ) shouldEqual Right(str)
      Decoder[Map[String, UUID]].read(
        Encoder[Map[String, UUID]].write(uuid)
      ) shouldEqual Right(uuid)
      Decoder[Map[String, Boolean]].read(
        Encoder[Map[String, Boolean]].write(bool)
      ) shouldEqual Right(bool)
      Decoder[Map[String, Long]].read(
        Encoder[Map[String, Long]].write(long)
      ) shouldEqual Right(long)
      Decoder[Map[String, Instant]].read(
        Encoder[Map[String, Instant]].write(instant)
      ) shouldEqual Right(instant)
  }
}
