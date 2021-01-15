package meteor

import cats.implicits._
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import meteor.codec._
import meteor.errors.DecoderError

trait syntax {

  implicit class AsAttributeValue[A: Encoder](a: A) {
    def asAttributeValue: AttributeValue = Encoder[A].write(a)
  }

  implicit class RichAttributeValue(av: AttributeValue) {

    def as[A: Decoder]: Either[DecoderError, A] = Decoder[A].read(av)

    def get(key: String): Either[DecoderError, AttributeValue] =
      if (av.hasM()) {
        av.m().getOrDefault(
          key,
          AttributeValue.builder().nul(true).build()
        ).asRight[DecoderError]
      } else {
        DecoderError.invalidTypeFailure(DynamoDbType.M).asLeft[AttributeValue]
      }

    def getAs[A: Decoder](key: String): Either[DecoderError, A] =
      get(key).flatMap(_.as[A])
  }
}

object syntax extends syntax
