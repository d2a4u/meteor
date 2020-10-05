package meteor
package codec

import java.{util => ju}
import java.time.Instant

import cats._
import cats.implicits._
import software.amazon.awssdk.services.dynamodb.model._

import scala.jdk.CollectionConverters._

trait Encoder[A] {
  def write(a: A): AttributeValue
}

object Encoder {
  def apply[A](implicit dd: Encoder[A]): Encoder[A] = dd

  def instance[A](f: A => AttributeValue): Encoder[A] = (a: A) => f(a)

  def const[A](av: AttributeValue): Encoder[A] = _ => av

  implicit def contravariantForDynamoDbDecoder: Contravariant[Encoder] =
    new Contravariant[Encoder] {
      def contramap[A, B](fa: Encoder[A])(f: B => A): Encoder[B] =
        (b: B) => fa.write(f(b))
    }

  implicit def dynamoEncoderForOption[A: Encoder]: Encoder[Option[A]] =
    Encoder.instance { fa =>
      fa.fold(AttributeValue.builder().nul(true).build())(Encoder[A].write)
    }

  implicit val dynamoEncoderForAttributeValue: Encoder[AttributeValue] =
    Encoder.instance(identity)

  implicit val dynamoEncoderForBoolean: Encoder[Boolean] =
    Encoder.instance(bool => AttributeValue.builder().bool(bool).build())

  implicit val dynamoEncoderForString: Encoder[String] =
    Encoder.instance(str => AttributeValue.builder().s(str).build())

  implicit val dynamoEncoderForUUID: Encoder[ju.UUID] =
    Encoder[String].contramap(_.toString)

  implicit val dynamoEncoderForLong: Encoder[Long] =
    Encoder.instance(long => AttributeValue.builder().n(long.toString).build())

  implicit val dynamoEncoderForInt: Encoder[Int] =
    Encoder.instance(int => AttributeValue.builder().n(int.toString).build())

  implicit val dynamoEncoderForInstant: Encoder[Instant] =
    dynamoEncoderForLong.contramap(instant => instant.toEpochMilli)

  implicit def dynamoEncoderForMap[A: Encoder]: Encoder[Map[String, A]] =
    Encoder.instance { mapOfA =>
      val mapOfAttr = mapOfA.view.mapValues(Encoder[A].write).toMap.asJava
      AttributeValue.builder().m(mapOfAttr).build()
    }
}
