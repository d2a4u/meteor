package meteor
package codec

import software.amazon.awssdk.services.dynamodb.model.AttributeValue

trait Codec[A] extends Decoder[A] with Encoder[A]

object Codec {

  def iso[A: Codec, B](fa: A => B)(fb: B => A)(implicit
  codecA: Codec[A]): Codec[B] =
    new Codec[B] {
      override def read(av: AttributeValue): Either[DecoderFailure, B] = {
        codecA.read(av).map(fa)
      }

      override def write(b: B): AttributeValue = {
        codecA.write(fb(b))
      }
    }
}
