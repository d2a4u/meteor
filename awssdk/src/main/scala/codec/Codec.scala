package meteor
package codec

import meteor.errors.DecoderError
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

/** Provides an encoding and a decoding functions for a given type.
  *
  * @tparam A
  */
trait Codec[A] extends Decoder[A] with Encoder[A]

object Codec {
  def apply[A](implicit codec: Codec[A]): Codec[A] = codec

  /** Returns a new [[Codec]] for the specified type given an [[Encoder]] and a [[Decoder]] in scope
    * for the type.
    */
  implicit def dynamoCodecFromEncoderAndDecoder[A](
    implicit encoder: Encoder[A],
    decoder: Decoder[A]
  ): Codec[A] =
    new Codec[A] {
      override def write(a: A): AttributeValue = encoder.write(a)

      override def read(av: AttributeValue): Either[DecoderError, A] =
        decoder.read(av)
    }

  /** Returns a new [[Codec]] of type B given isomorphic functions of A to B and B to A
    */
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
