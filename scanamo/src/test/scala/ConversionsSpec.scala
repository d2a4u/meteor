package meteor
package scanamo
package formats

import meteor.codec.{Codec, Decoder, Encoder}
import meteor.syntax._
import meteor.scanamo.formats.conversions._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scanamo.DynamoReadError
import org.scanamo.{DynamoFormat, DynamoValue}

import scala.collection.immutable

class ConversionsSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {
  behavior.of("DynamoFormat conversion")

  implicit val genNonEmptyString: Gen[String] =
    Gen.nonEmptyListOf(Gen.alphaNumChar).map(_.mkString(""))

  implicit val arbNonEmptyString: Arbitrary[String] =
    Arbitrary(genNonEmptyString)

  def roundTrip[T: Codec: DynamoFormat](t: T): Boolean = {
    val dynamoFormatWrite = DynamoFormat[T].write(t).toAttributeValue
    val encoderWrite = Encoder[T].write(t)

    val round1 = Decoder[T].read(dynamoFormatWrite).toOption
    val round2 = DynamoFormat[T].read(encoderWrite).toOption
    round1.get == round2.get
  }

  def roundTripOpt[T: Decoder](
    t: Option[T]
  )(implicit enc: Encoder[Option[T]], fmt: DynamoFormat[Option[T]]): Boolean = {
    val dynamoFormatWrite = fmt.write(t).toAttributeValue
    val encoderWrite = enc.write(t)

    val round1 = dynamoFormatWrite.asOpt[T].toOption
    val round2 = fmt.read(encoderWrite).toOption
    round1.flatten == round2.flatten
  }

  it should "cross read/write from DynamoFormat and Encoder/Decoder for Int" in forAll {
    int: Int =>
      roundTrip(int) shouldBe true
  }

  it should "cross read/write from DynamoFormat and Encoder/Decoder for non empty String" in forAll {
    str: String =>
      roundTrip(str) shouldBe true
  }

  // test to demonstrate Scanamo's issue where it writes None and Some("") both to Dynamo' null, which
  // throws away original type and make it not possible to figure out which is which when reading
  it should "scanano cannot distinguish between None and Some of empty String" ignore {
    val format = DynamoFormat[Option[String]]
    format.read(format.write(None)) shouldEqual Right(None)
    format.read(format.write(Some(""))) shouldEqual Right(Some(""))
  }

  it should "cross read/write from DynamoFormat and Encoder/Decoder for Boolean" in forAll {
    bool: Boolean =>
      roundTrip(bool) shouldBe true
  }

  it should "cross read/write from DynamoFormat and Encoder/Decoder for Long" in forAll {
    long: Long =>
      roundTrip(long) shouldBe true
  }

  it should "cross read/write from DynamoFormat and Encoder/Decoder for Float" in forAll {
    float: Float =>
      roundTrip(float) shouldBe true
  }

  it should "cross read/write from DynamoFormat and Encoder/Decoder for Byte" in forAll {
    byte: Byte =>
      roundTrip(byte) shouldBe true
  }

  it should "cross read/write from DynamoFormat and Encoder/Decoder for Double" in forAll {
    double: Double =>
      roundTrip(double) shouldBe true
  }

  it should "cross read/write from DynamoFormat and Encoder/Decoder for Short" in forAll {
    short: Short =>
      roundTrip(short) shouldBe true
  }

  it should "cross read/write from DynamoFormat and Encoder/Decoder for BigDecimal" in forAll {
    bd: BigDecimal =>
      roundTrip(bd) shouldBe true
  }

  it should "cross read/write from DynamoFormat and Encoder/Decoder for Option[Int]" in forAll {
    opt: Option[Int] =>
      roundTripOpt(opt) shouldBe true
  }

  it should "cross read/write from DynamoFormat and Encoder/Decoder for List[Int]" in forAll {
    list: List[Int] =>
      roundTrip(list) shouldBe true
  }

  val seqFmt = implicitly[DynamoFormat[Seq[Int]]]

  it should "cross read/write from DynamoFormat and Encoder/Decoder for Seq[Int]" in forAll {
    seq: immutable.Seq[Int] =>
      implicit val immutableFmt: DynamoFormat[immutable.Seq[Int]] =
        new DynamoFormat[immutable.Seq[Int]] {
          override def read(av: DynamoValue)
            : Either[DynamoReadError, immutable.Seq[Int]] =
            seqFmt.read(av).map(_.toList)

          override def write(t: immutable.Seq[Int]): DynamoValue =
            seqFmt.write(t)
        }
      implicitly[DynamoFormat[Seq[Int]]]
      roundTrip(seq) shouldBe true
  }

  it should "cross read/write from DynamoFormat and Encoder/Decoder for Map[String, Int]" in forAll {
    map: Map[String, Int] =>
      roundTrip(map) shouldBe true
  }
}
