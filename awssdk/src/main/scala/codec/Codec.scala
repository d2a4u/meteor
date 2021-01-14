package meteor
package codec

import meteor.errors.DecoderError
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

trait Codec[A] extends Decoder[A] with Encoder[A]

object Codec {

  def apply[A](implicit codec: Codec[A]): Codec[A] = codec

  implicit def dynamoCodecFromEncoderAndDecoder[A](
    implicit encoder: Encoder[A],
    decoder: Decoder[A]
  ): Codec[A] =
    new Codec[A] {
      override def write(a: A): AttributeValue = encoder.write(a)

      override def read(av: AttributeValue): Either[DecoderError, A] =
        decoder.read(av)
    }

  def iso[A: Codec, B](fa: A => B)(fb: B => A): Codec[B] =
    new Codec[B] {
      override def read(av: AttributeValue): Either[DecoderError, B] = {
        Codec[A].read(av).map(fa)
      }

      override def write(b: B): AttributeValue = {
        Codec[A].write(fb(b))
      }
    }

}
