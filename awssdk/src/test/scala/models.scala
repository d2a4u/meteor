package meteor

import meteor.codec.{Decoder, Encoder}
import org.scalacheck.{Arbitrary, Gen}
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import scala.jdk.CollectionConverters._

case class Id(value: String) extends AnyVal
object Id {
  implicit val encoderId: Encoder[Id] = Encoder.instance { id =>
    AttributeValue.builder().m(
      Map("id" -> Encoder[String].write(id.value)).asJava
    ).build()
  }

  implicit val decoderId: Decoder[Id] = Decoder.instance { av =>
    val obj = av.m()
    Decoder[String].read(obj.get("id")).map(Id.apply)
  }

  implicit val genId: Gen[Id] =
    Gen.nonEmptyListOf(Gen.alphaNumChar).map(chars => Id(chars.mkString))
  implicit val arbId: Arbitrary[Id] = Arbitrary(genId)
}

case class Range(value: String) extends AnyVal
object Range {
  implicit val encoderRange: Encoder[Range] = Encoder.instance { range =>
    AttributeValue.builder().m(
      Map("range" -> Encoder[String].write(range.value)).asJava
    ).build()
  }

  implicit val decoderRange: Decoder[Range] = Decoder.instance { av =>
    val obj = av.m()
    Decoder[String].read(obj.get("range")).map(Range.apply)
  }

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
    val obj = av.m()
    for {
      id <- Decoder[String].read(obj.get("id"))
      range <- Decoder[String].read(obj.get("range"))
      str <- Decoder[String].read(obj.get("str"))
      int <- Decoder[Int].read(obj.get("int"))
      bool <- Decoder[Boolean].read(obj.get("bool"))
    } yield TestData(Id(id), Range(range), str, int, bool)
  }

  implicit val encoder: Encoder[TestData] = Encoder.instance { t =>
    val jMap = Map(
      "id" -> Encoder[String].write(t.id.value),
      "range" -> Encoder[String].write(t.range.value),
      "str" -> Encoder[String].write(t.str),
      "int" -> Encoder[Int].write(t.int),
      "bool" -> Encoder[Boolean].write(t.bool)
    ).asJava
    AttributeValue.builder().m(jMap).build()
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
