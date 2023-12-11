package meteor.dynosaur
package formats

import dynosaur.Schema
import meteor.codec.Codec.dynamoCodecFromEncoderAndDecoder
import meteor.codec.{Codec, Decoder, Encoder}
import meteor.syntax._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.collection.immutable

class ConversionsSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {
  behavior.of("Schema conversion")

  implicit val genNonEmptyString: Gen[String] =
    Gen.nonEmptyListOf(Gen.alphaNumChar).map(_.mkString(""))

  implicit val arbNonEmptyString: Arbitrary[String] =
    Arbitrary(genNonEmptyString)

  def roundTrip[T: Codec: Schema](t: T): Boolean = {
    val convertedCodec = conversions.schemaToCodec(Schema[T])
    val round1 = Codec[T].read(convertedCodec.write(t))
    val round2 = convertedCodec.read(Codec[T].write(t))
    round1.isRight && round1 == round2
  }

  def roundTripOpt[T: Decoder](
    encoder: Encoder[Option[T]],
    schema: Schema[Option[T]],
    t: Option[T]
  ): Boolean = {
    val convertedCodec = conversions.schemaToCodec(schema)
    val round1 = convertedCodec.write(t).asOpt[T]
    val round2 = convertedCodec.read(encoder.write(t))
    round1.isRight && round1 == round2
  }

  //TODO: test for Array[Byte]
  it should "cross read/write from Schema to Codec for Int" in forAll {
    (int: Int) =>
      roundTrip(int) shouldBe true
  }

  it should "cross read/write from Schema to Codec for non empty String" in forAll {
    (str: String) =>
      roundTrip(str) shouldBe true
  }

  it should "cross read/write from Schema to Codec for Boolean" in forAll {
    (bool: Boolean) =>
      roundTrip(bool) shouldBe true
  }

  it should "cross read/write from Schema to Codec for Long" in forAll {
    (long: Long) =>
      roundTrip(long) shouldBe true
  }

  it should "cross read/write from Schema to Codec for Float" in forAll {
    (float: Float) =>
      roundTrip(float) shouldBe true
  }

  it should "cross read/write from Schema to Codec for Double" in forAll {
    (double: Double) =>
      roundTrip(double) shouldBe true
  }

  it should "cross read/write from Schema to Codec for Short" in forAll {
    (short: Short) =>
      roundTrip(short) shouldBe true
  }

  it should "cross read/write from Schema to Codec for Option[Int]" in forAll {
    (opt: Option[Int]) =>
      roundTripOpt[Int](
        Encoder[Option[Int]],
        Schema.nullable[Int],
        opt
      ) shouldBe true
  }

  it should "cross read/write from Schema to Codec for List[Int]" in forAll {
    (list: List[Int]) =>
      roundTrip(list) shouldBe true
  }

  it should "cross read/write from Schema to Codec for Seq[Int]" in forAll {
    (seq: immutable.Seq[Int]) =>
      roundTrip(seq) shouldBe true
  }

  it should "cross read/write from Schema to Codec for Map[String, Int]" in forAll {
    (map: Map[String, Int]) =>
      roundTrip(map) shouldBe true
  }
}
