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

  implicit val genRange: Gen[Range] =
    Gen.nonEmptyListOf(Gen.alphaNumChar).map(chars => Range(chars.mkString))
  implicit val arbRange: Arbitrary[Range] = Arbitrary(genRange)
}

case class TestData(
  id: Id,
  range: Range,
  data: String
)
object TestData {
  implicit val decoder: Decoder[TestData] = Decoder.instance { av =>
    val obj = av.m()
    for {
      id <- Decoder[String].read(obj.get("id"))
      range <- Decoder[String].read(obj.get("range"))
      data <- Decoder[String].read(obj.get("data"))
    } yield TestData(Id(id), Range(range), data)
  }

  implicit val encoder: Encoder[TestData] = Encoder.instance { t =>
    val jMap = Map(
      "id" -> Encoder[String].write(t.id.value),
      "range" -> Encoder[String].write(t.range.value),
      "data" -> Encoder[String].write(t.data)
    ).asJava
    AttributeValue.builder().m(jMap).build()
  }

  implicit val genTestData: Gen[TestData] =
    for {
      id <- implicitly[Gen[Id]]
      range <- implicitly[Gen[Range]]
      data <- Gen.asciiPrintableStr
    } yield TestData(id, range, data)

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

  implicit val encoder: Encoder[TestDataSimple] = Encoder.instance { t =>
    val jMap = Map(
      "id" -> Encoder[String].write(t.id.value),
      "data" -> Encoder[String].write(t.data)
    ).asJava
    AttributeValue.builder().m(jMap).build()
  }

  implicit val genTestDataSimple: Gen[TestDataSimple] =
    for {
      id <- implicitly[Gen[Id]]
      data <- Gen.asciiPrintableStr
    } yield TestDataSimple(id, data)

  implicit val arbTestDataSimple: Arbitrary[TestDataSimple] =
    Arbitrary(genTestDataSimple)
}
