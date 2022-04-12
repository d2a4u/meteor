package meteor.dynosaur
package formats

import cats.implicits._
import dynosaur.{DynamoValue, Schema}
import meteor.codec.Codec
import meteor.errors.DecoderError
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

object conversions {
  implicit def schemaToCodec[A](implicit schema: Schema[A]): Codec[A] = {
    new Codec[A] {
      override def write(a: A): AttributeValue = {
        schema.write(a).fold(
          e => throw e,
          _.value
        )
      }

      override def read(av: AttributeValue): Either[DecoderError, A] = {
        val dv = DynamoValue(av)
        schema.read(dv).leftMap { err =>
          DecoderError(err.getMessage, err.getCause.some)
        }
      }
    }
  }
}
