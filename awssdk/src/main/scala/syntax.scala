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

    def asOpt[A: Decoder]: Either[DecoderError, Option[A]] =
      if (Option(av.nul()).exists(_.booleanValue())) None.asRight
      else av.as[A].map(_.some)

    def get(key: String): Either[DecoderError, AttributeValue] =
      if (av.hasM) {
        Option(av.m().get(key)).fold(
          DecoderError.missingKeyFailure(key).asLeft[AttributeValue]
        )(_.asRight[DecoderError])
      } else {
        DecoderError.invalidTypeFailure(DynamoDbType.M).asLeft[AttributeValue]
      }

    def getAs[A: Decoder](key: String): Either[DecoderError, A] =
      get(key).flatMap(_.as[A])

    def getOpt[A: Decoder](key: String): Either[DecoderError, Option[A]] =
      if (av.hasM) {
        Option(av.m().get(key)).fold(none[A].asRight[DecoderError])(_.asOpt[A])
      } else {
        DecoderError.invalidTypeFailure(DynamoDbType.M).asLeft[Option[A]]
      }
  }
}

object syntax extends syntax
