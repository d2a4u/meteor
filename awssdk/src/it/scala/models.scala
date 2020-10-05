package meteor

import meteor.codec.{Decoder, Encoder}
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import scala.jdk.CollectionConverters._

case class Id(value: String) extends AnyVal
object Id {
  implicit val idEncoder: Encoder[Id] = Encoder.instance { id =>
    AttributeValue.builder().m(
      Map("id" -> Encoder[String].write(id.value)).asJava
    ).build()
  }
}

case class Range(value: String) extends AnyVal
object Range {
  implicit val rangeEncoder: Encoder[Range] = Encoder.instance { range =>
    AttributeValue.builder().m(
      Map("range" -> Encoder[String].write(range.value)).asJava
    ).build()
  }
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
}
