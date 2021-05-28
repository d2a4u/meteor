package meteor

import meteor.syntax._
import meteor.codec.{Codec, Decoder, Encoder}
import org.scalacheck.{Arbitrary, Gen}
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import scala.jdk.CollectionConverters._

case class Id(value: String) extends AnyVal
object Id {
  implicit val codecId: Codec[Id] = Codec.iso[String, Id](Id.apply)(_.value)

  implicit val genId: Gen[Id] =
    Gen.nonEmptyListOf(Gen.alphaNumChar).map(chars => Id(chars.mkString))
  implicit val arbId: Arbitrary[Id] = Arbitrary(genId)
}

case class Range(value: String) extends AnyVal
object Range {
  implicit val codecRange: Codec[Range] =
    Codec.iso[String, Range](Range.apply)(_.value)

  implicit val genRange: Gen[Range] =
    Gen.nonEmptyListOf(Gen.alphaNumChar).map(chars => Range(chars.mkString))
  implicit val arbRange: Arbitrary[Range] = Arbitrary(genRange)
}

case class TestData(
  id: Id,
  range: Range,
  str: String,
  int: Int,
  bool: Boolean
)
object TestData {
  implicit val decoder: Decoder[TestData] = Decoder.instance { av =>
    for {
      id <- av.getAs[String]("id")
      range <- av.getAs[String]("range")
      str <- av.getAs[String]("str")
      int <- av.getAs[Int]("int")
      bool <- av.getAs[Boolean]("bool")
    } yield TestData(Id(id), Range(range), str, int, bool)
  }

  implicit val encoder: Encoder[TestData] = Encoder.instance { t =>
    Map(
      "id" -> t.id.value.asAttributeValue,
      "range" -> t.range.value.asAttributeValue,
      "str" -> t.str.asAttributeValue,
      "int" -> t.int.asAttributeValue,
      "bool" -> t.bool.asAttributeValue
    ).asAttributeValue
  }

  implicit val genTestData: Gen[TestData] =
    for {
      id <- implicitly[Gen[Id]]
      range <- implicitly[Gen[Range]]
      str <- Gen.asciiPrintableStr
      int <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
      bool <- Gen.oneOf(Seq(true, false))
    } yield TestData(id, range, str, int, bool)

  implicit val arbTestData: Arbitrary[TestData] = Arbitrary(genTestData)
}

case class TestDataSimple(
  id: Id,
  data: String
)
object TestDataSimple {
  implicit val decoder: Decoder[TestDataSimple] = Decoder.instance { av =>
    val obj = av.m()
    for {
      id <- Decoder[String].read(obj.get("id"))
      data <- Decoder[String].read(obj.get("data"))
    } yield TestDataSimple(Id(id), data)
  }

  implicit def encoder: Encoder[TestDataSimple] =
    Encoder.instance { t =>
      val jMap = Map(
        "id" -> Encoder[String].write(t.id.value),
        "data" -> Encoder[String].write(t.data)
      ).asJava
      AttributeValue.builder().m(jMap).build()
    }

  implicit val genTestDataSimple: Gen[TestDataSimple] =
    TestData.genTestData.map(data => TestDataSimple(data.id, data.str))

  implicit val arbTestDataSimple: Arbitrary[TestDataSimple] =
    Arbitrary(genTestDataSimple)
}
