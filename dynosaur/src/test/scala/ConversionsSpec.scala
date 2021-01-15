package meteor.dynosaur
package formats

import dynosaur.Schema
import meteor.codec.Codec
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

  def roundTrip[T](codec: Codec[T], schema: Schema[T], t: T): Boolean = {
    val convertedCodec = conversions.schemaToCodec(schema)
    val round1 = codec.read(convertedCodec.write(t))
    val round2 = convertedCodec.read(codec.write(t))
    round1.isRight && round1 == round2
  }

  //TODO: test for Array[Byte]
  it should "cross read/write from Schema to Codec for Int" in forAll {
    int: Int =>
      roundTrip[Int](Codec[Int], Schema.int, int) shouldBe true
  }

  it should "cross read/write from Schema to Codec for non empty String" in forAll {
    str: String =>
      roundTrip[String](Codec[String], Schema.string, str) shouldBe true
  }

  it should "cross read/write from Schema to Codec for Boolean" in forAll {
    bool: Boolean =>
      roundTrip[Boolean](Codec[Boolean], Schema.boolean, bool) shouldBe true
  }

  it should "cross read/write from Schema to Codec for Long" in forAll {
    long: Long =>
      roundTrip[Long](Codec[Long], Schema.long, long) shouldBe true
  }

  it should "cross read/write from Schema to Codec for Float" in forAll {
    float: Float =>
      roundTrip[Float](Codec[Float], Schema.float, float) shouldBe true
  }

  it should "cross read/write from Schema to Codec for Double" in forAll {
    double: Double =>
      roundTrip[Double](Codec[Double], Schema.double, double) shouldBe true
  }

  it should "cross read/write from Schema to Codec for Short" in forAll {
    short: Short =>
      roundTrip[Short](Codec[Short], Schema.short, short) shouldBe true
  }

  it should "cross read/write from Schema to Codec for Option[Int]" in forAll {
    opt: Option[Int] =>
      roundTrip[Option[Int]](
        Codec[Option[Int]],
        Schema.nullable[Int],
        opt
      ) shouldBe true
  }

  it should "cross read/write from Schema to Codec for List[Int]" in forAll {
    list: List[Int] =>
      roundTrip[List[Int]](
        Codec[List[Int]],
        Schema.list[Int],
        list
      ) shouldBe true
  }

  it should "cross read/write from Schema to Codec for Seq[Int]" in forAll {
    seq: immutable.Seq[Int] =>
      roundTrip[immutable.Seq[Int]](
        Codec[immutable.Seq[Int]],
        Schema.seq[Int],
        seq
      ) shouldBe true
  }

  it should "cross read/write from Schema to Codec for Map[String, Int]" in forAll {
    map: Map[String, Int] =>
      roundTrip[Map[String, Int]](
        Codec[Map[String, Int]],
        Schema.dict[Int],
        map
      ) shouldBe true
  }
}
