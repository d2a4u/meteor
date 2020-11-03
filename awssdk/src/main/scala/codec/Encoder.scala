package meteor
package codec

import java.{util => ju}
import java.time.Instant

import cats._
import cats.implicits._
import software.amazon.awssdk.services.dynamodb.model._

import meteor.implicits._

import scala.jdk.CollectionConverters._

trait Encoder[A] {
  def write(a: A): AttributeValue
}

object Encoder {
  def apply[A](implicit dd: Encoder[A]): Encoder[A] = dd

  def instance[A](f: A => AttributeValue): Encoder[A] = (a: A) => f(a)

  def const[A](key: String, value: String): Encoder[A] =
    _ =>
      AttributeValue
        .builder()
        .m(
          Map(key -> Encoder[String].write(value)).asJava
        )
        .build()

  def const[A](av: AttributeValue): Encoder[A] = _ => av

  implicit def contravariantForDynamoDbDecoder: Contravariant[Encoder] =
    new Contravariant[Encoder] {
      def contramap[A, B](fa: Encoder[A])(f: B => A): Encoder[B] =
        (b: B) => fa.write(f(b))
    }

  implicit def dynamoEncoderForTuple2[A: Encoder, B: Encoder]: Encoder[(A, B)] =
    Encoder.instance { ab =>
      val (a, b) = ab
      val writeA = Encoder[A].write(a)
      val writeB = Encoder[B].write(b)
      val m = writeA.m() ++ writeB.m()
      dynamoEncoderForMap[AttributeValue].write(m.asScala.toMap)
    }

  implicit def dynamoEncoderForEither[A: Encoder, B: Encoder]
    : Encoder[Either[A, B]] =
    Encoder.instance {
      case Right(r) => Encoder[B].write(r)

      case Left(l) => Encoder[A].write(l)
    }

  implicit def dynamoEncoderForOption[A: Encoder]: Encoder[Option[A]] =
    Encoder.instance { fa =>
      fa.fold(AttributeValue.builder().nul(true).build())(Encoder[A].write)
    }

  implicit def dynamoEncoderForSeq[A: Encoder]: Encoder[Seq[A]] =
    Encoder.instance { fa =>
      AttributeValue.builder().l(fa.map(Encoder[A].write): _*).build()
    }

  implicit def dynamoEncoderForList[A: Encoder]: Encoder[List[A]] =
    dynamoEncoderForSeq[A].contramap(_.toSeq)

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

  implicit val dynamoEncoderForFloat: Encoder[Float] =
    Encoder.instance(float =>
      AttributeValue.builder().n(float.toString).build())

  implicit val dynamoEncoderForDouble: Encoder[Double] =
    Encoder.instance(double =>
      AttributeValue.builder().n(double.toString).build())

  implicit val dynamoEncoderForBigDecimal: Encoder[BigDecimal] =
    Encoder.instance(bd => AttributeValue.builder().n(bd.toString).build())

  implicit val dynamoEncoderForShort: Encoder[Short] =
    Encoder.instance(short =>
      AttributeValue.builder().n(short.toString).build())

  implicit val dynamoEncoderForByte: Encoder[Byte] =
    Encoder.instance(byte => AttributeValue.builder().n(byte.toString).build())

  implicit val dynamoEncoderForInt: Encoder[Int] =
    Encoder.instance(int => AttributeValue.builder().n(int.toString).build())

  implicit val dynamoEncoderForInstant: Encoder[Instant] =
    dynamoEncoderForLong.contramap(instant => instant.toEpochMilli)

  implicit def dynamoEncoderForMap[A: Encoder]: Encoder[Map[String, A]] =
    Encoder.instance { mapOfA =>
      val mapOfAttr =
        mapOfA.map(kv => kv._1 -> Encoder[A].write(kv._2)).asJava
      AttributeValue.builder().m(mapOfAttr).build()
    }
}
