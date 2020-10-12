package meteor
package scanamo
package formats

import meteor.codec.{Decoder, Encoder}
import meteor.scanamo.formats.conversions._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scanamo.DynamoFormat

class ConversionsSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {
  behavior.of("DynamoFormat conversion")

  implicit val genNonEmptyString: Gen[String] =
    Gen.nonEmptyListOf(Gen.alphaNumChar).map(_.mkString(""))

  implicit val arbNonEmptyString: Arbitrary[String] =
    Arbitrary(genNonEmptyString)

  it should "cross read/write from DynamoFormat and Encoder/Decoder for Int" in forAll {
    int: Int =>
      val dynamoFormatWrite =
        sdk1ToSdk2AttributeValue(
          DynamoFormat[Int].write(int).toAttributeValue
        )
      val encoderWrite =
        sdk2ToSdk1AttributeValue(Encoder[Int].write(int))

      Decoder[Int].read(dynamoFormatWrite) shouldEqual Right(int)
      DynamoFormat[Int].read(encoderWrite) shouldEqual Right(int)
  }

  it should "cross read/write from DynamoFormat and Encoder/Decoder for non empty String" in forAll {
    str: String =>
      val dynamoFormatWrite =
        sdk1ToSdk2AttributeValue(
          DynamoFormat[String].write(str).toAttributeValue
        )
      val encoderWrite =
        sdk2ToSdk1AttributeValue(Encoder[String].write(str))

      Decoder[String].read(dynamoFormatWrite) shouldEqual Right(str)
      DynamoFormat[String].read(encoderWrite) shouldEqual Right(str)
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
      val dynamoFormatWrite =
        sdk1ToSdk2AttributeValue(
          DynamoFormat[Boolean].write(bool).toAttributeValue
        )
      val encoderWrite =
        sdk2ToSdk1AttributeValue(Encoder[Boolean].write(bool))

      Decoder[Boolean].read(dynamoFormatWrite) shouldEqual Right(bool)
      DynamoFormat[Boolean].read(encoderWrite) shouldEqual Right(bool)
  }

  it should "cross read/write from DynamoFormat and Encoder/Decoder for Long" in forAll {
    long: Long =>
      val dynamoFormatWrite =
        sdk1ToSdk2AttributeValue(
          DynamoFormat[Long].write(long).toAttributeValue
        )
      val encoderWrite =
        sdk2ToSdk1AttributeValue(Encoder[Long].write(long))

      Decoder[Long].read(dynamoFormatWrite) shouldEqual Right(long)
      DynamoFormat[Long].read(encoderWrite) shouldEqual Right(long)
  }

  it should "cross read/write from DynamoFormat and Encoder/Decoder for Float" in forAll {
    float: Float =>
      val dynamoFormatWrite =
        sdk1ToSdk2AttributeValue(
          DynamoFormat[Float].write(float).toAttributeValue
        )
      val encoderWrite =
        sdk2ToSdk1AttributeValue(Encoder[Float].write(float))

      Decoder[Float].read(dynamoFormatWrite) shouldEqual Right(float)
      DynamoFormat[Float].read(encoderWrite) shouldEqual Right(float)
  }

  it should "cross read/write from DynamoFormat and Encoder/Decoder for Byte" in forAll {
    byte: Byte =>
      val dynamoFormatWrite =
        sdk1ToSdk2AttributeValue(
          DynamoFormat[Byte].write(byte).toAttributeValue
        )
      val encoderWrite =
        sdk2ToSdk1AttributeValue(Encoder[Byte].write(byte))

      Decoder[Byte].read(dynamoFormatWrite) shouldEqual Right(byte)
      DynamoFormat[Byte].read(encoderWrite) shouldEqual Right(byte)
  }

  it should "cross read/write from DynamoFormat and Encoder/Decoder for Double" in forAll {
    double: Double =>
      val dynamoFormatWrite =
        sdk1ToSdk2AttributeValue(
          DynamoFormat[Double].write(double).toAttributeValue
        )
      val encoderWrite =
        sdk2ToSdk1AttributeValue(Encoder[Double].write(double))

      Decoder[Double].read(dynamoFormatWrite) shouldEqual Right(double)
      DynamoFormat[Double].read(encoderWrite) shouldEqual Right(double)
  }

  it should "cross read/write from DynamoFormat and Encoder/Decoder for Short" in forAll {
    short: Short =>
      val dynamoFormatWrite =
        sdk1ToSdk2AttributeValue(
          DynamoFormat[Short].write(short).toAttributeValue
        )
      val encoderWrite =
        sdk2ToSdk1AttributeValue(Encoder[Short].write(short))

      Decoder[Short].read(dynamoFormatWrite) shouldEqual Right(short)
      DynamoFormat[Short].read(encoderWrite) shouldEqual Right(short)
  }

  it should "cross read/write from DynamoFormat and Encoder/Decoder for BigDecimal" in forAll {
    bd: BigDecimal =>
      val dynamoFormatWrite =
        sdk1ToSdk2AttributeValue(
          DynamoFormat[BigDecimal].write(bd).toAttributeValue
        )
      val encoderWrite =
        sdk2ToSdk1AttributeValue(Encoder[BigDecimal].write(bd))

      Decoder[BigDecimal].read(dynamoFormatWrite) shouldEqual Right(bd)
      DynamoFormat[BigDecimal].read(encoderWrite) shouldEqual Right(bd)
  }

  it should "cross read/write from DynamoFormat and Encoder/Decoder for Option[Int]" in forAll {
    opt: Option[Int] =>
      val dynamoFormatWrite =
        sdk1ToSdk2AttributeValue(
          DynamoFormat[Option[Int]].write(opt).toAttributeValue
        )
      val encoderWrite =
        sdk2ToSdk1AttributeValue(
          Encoder[Option[Int]].write(opt)
        )

      Decoder[Option[Int]].read(dynamoFormatWrite) shouldEqual Right(
        opt
      )
      DynamoFormat[Option[Int]].read(encoderWrite) shouldEqual Right(
        opt
      )
  }

  it should "cross read/write from DynamoFormat and Encoder/Decoder for List[Int]" in forAll {
    list: List[Int] =>
      val dynamoFormatWrite =
        sdk1ToSdk2AttributeValue(
          DynamoFormat[List[Int]].write(list).toAttributeValue
        )
      val encoderWrite =
        sdk2ToSdk1AttributeValue(Encoder[List[Int]].write(list))

      Decoder[List[Int]].read(dynamoFormatWrite) shouldEqual Right(
        list
      )
      DynamoFormat[List[Int]].read(encoderWrite) shouldEqual Right(
        list
      )
  }

  it should "cross read/write from DynamoFormat and Encoder/Decoder for Seq[Int]" in forAll {
    seq: Seq[Int] =>
      val dynamoFormatWrite =
        sdk1ToSdk2AttributeValue(
          DynamoFormat[Seq[Int]].write(seq).toAttributeValue
        )
      val encoderWrite =
        sdk2ToSdk1AttributeValue(Encoder[Seq[Int]].write(seq))

      Decoder[Seq[Int]].read(dynamoFormatWrite) shouldEqual Right(
        seq
      )
      DynamoFormat[Seq[Int]].read(encoderWrite) shouldEqual Right(
        seq
      )
  }

  it should "cross read/write from DynamoFormat and Encoder/Decoder for Map[String, Int]" in forAll {
    map: Map[String, Int] =>
      val dynamoFormatWrite =
        sdk1ToSdk2AttributeValue(
          DynamoFormat[Map[String, Int]].write(map).toAttributeValue
        )
      val encoderWrite =
        sdk2ToSdk1AttributeValue(Encoder[Map[String, Int]].write(map))

      Decoder[Map[String, Int]].read(dynamoFormatWrite) shouldEqual Right(
        map
      )
      DynamoFormat[Map[String, Int]].read(encoderWrite) shouldEqual Right(
        map
      )
  }
}
