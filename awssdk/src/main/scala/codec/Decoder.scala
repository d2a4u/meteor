package meteor
package codec

import java.time.Instant
import java.{util => ju}

import cats._
import cats.implicits._
import meteor.codec.primitives.DynamoDbType
import software.amazon.awssdk.services.dynamodb.model._

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

case class DecoderFailure(message: String, cause: Option[Throwable] = None)
    extends RuntimeException(message, cause.orNull)

object DecoderFailure {
  def invalidTypeFailure(t: DynamoDbType): DecoderFailure =
    DecoderFailure(s"The AttributeValue must be of type ${t.show}")

  val nullValue: DecoderFailure =
    DecoderFailure("Returned value is null")
}

trait Decoder[A] {
  def read(av: AttributeValue): Either[DecoderFailure, A]
  def read(av: java.util.Map[String, AttributeValue])
    : Either[DecoderFailure, A] =
    read(AttributeValue.builder().m(av).build())
  def emap[B](f: A => Either[DecoderFailure, B]): Decoder[B] =
    Decoder.instance { av =>
      this.read(av).flatMap(f)
    }
}

object Decoder {
  type FailureOr[U] = Either[DecoderFailure, U]

  def apply[A](implicit dd: Decoder[A]): Decoder[A] = dd

  def instance[A](f: AttributeValue => Either[DecoderFailure, A]): Decoder[A] =
    (av: AttributeValue) => f(av)

  def failed[A](failure: DecoderFailure): Decoder[A] =
    _ => failure.asLeft[A]

  def const[A](a: A): Decoder[A] = _ => a.asRight[DecoderFailure]

  implicit def monadForDynamoDbDecoder: Monad[Decoder] =
    new Monad[Decoder] {

      def pure[A](x: A): Decoder[A] =
        Decoder.instance[A](_ => x.asRight[DecoderFailure])

      def flatMap[A, B](fa: Decoder[A])(f: A => Decoder[B]): Decoder[B] =
        (av: AttributeValue) => fa.read(av).flatMap(a => f(a).read(av))

      def tailRecM[A, B](init: A)(f: A => Decoder[Either[A, B]]): Decoder[B] =
        new Decoder[B] {
          @tailrec
          private def step(
            av: AttributeValue,
            a: A
          ): Either[DecoderFailure, B] =
            f(a).read(av) match {
              case l @ Left(_)     => l.rightCast[B]
              case Right(Left(a2)) => step(av, a2)
              case Right(Right(b)) => Right(b)
            }

          def read(av: AttributeValue): Either[DecoderFailure, B] =
            step(av, init)
        }
    }

  implicit val dynamoDecoderForAttributeValue: Decoder[AttributeValue] =
    Decoder.instance(av => av.asRight[DecoderFailure])

  implicit def dynamoDecoderForTuple2[A: Decoder, B: Decoder]: Decoder[(A, B)] =
    Decoder.instance { av =>
      (for {
        a <- Decoder[A].read(av)
        b <- Decoder[B].read(av)
      } yield (a, b)).leftMap(_ =>
        DecoderFailure.invalidTypeFailure(DynamoDbType.M))
    }

  implicit def dynamoDecoderForOption[A: Decoder]: Decoder[Option[A]] =
    Decoder.instance { av =>
      if (Option(av.nul()).exists(_.booleanValue())) {
        none[A].asRight[DecoderFailure]
      } else {
        Decoder[A].read(av).map(_.some)
      }
    }

  implicit def dynamoDecoderForSeq[A: Decoder]: Decoder[Seq[A]] =
    Decoder.instance { av =>
      if (av.hasL) {
        av.l().asScala.toList.traverse[FailureOr, A](Decoder[A].read)
      } else {
        DecoderFailure.invalidTypeFailure(DynamoDbType.L).asLeft[Seq[A]]
      }
    }

  implicit def dynamoDecoderForList[A: Decoder]: Decoder[List[A]] =
    dynamoDecoderForSeq[A].map(_.toList)

  implicit val dynamoDecoderForString: Decoder[String] =
    Decoder.instance { av =>
      Option(av.s()).toRight(DecoderFailure.invalidTypeFailure(DynamoDbType.S))
    }

  implicit val dynamoDecoderForUUID: Decoder[ju.UUID] =
    Decoder[String].flatMap {
      str =>
        Either
          .catchNonFatal(ju.UUID.fromString(str))
          .fold(
            e => Decoder.failed(DecoderFailure(e.getMessage, e.some)),
            ok => Decoder.const(ok)
          )
    }

  implicit val dynamoDecoderForBoolean: Decoder[Boolean] =
    Decoder.instance { av =>
      Option(av.bool())
        .map(_.booleanValue())
        .toRight(DecoderFailure.invalidTypeFailure(DynamoDbType.BOOL))
    }

  implicit val dynamoDecoderForLong: Decoder[Long] =
    Decoder.instance { av =>
      Option(av.n())
        .toRight(DecoderFailure.invalidTypeFailure(DynamoDbType.N))
        .flatMap(n =>
          Either.catchNonFatal(n.toLong).leftMap(e =>
            DecoderFailure(e.getMessage, e.some)))
    }

  implicit val dynamoDecoderForFloat: Decoder[Float] =
    Decoder.instance { av =>
      Option(av.n())
        .toRight(DecoderFailure.invalidTypeFailure(DynamoDbType.N))
        .flatMap(n =>
          Either.catchNonFatal(n.toFloat).leftMap(e =>
            DecoderFailure(e.getMessage, e.some)))
    }

  implicit val dynamoDecoderForDouble: Decoder[Double] =
    Decoder.instance { av =>
      Option(av.n())
        .toRight(DecoderFailure.invalidTypeFailure(DynamoDbType.N))
        .flatMap(n =>
          Either.catchNonFatal(n.toDouble).leftMap(e =>
            DecoderFailure(e.getMessage, e.some)))
    }

  implicit val dynamoDecoderForBigDecimal: Decoder[BigDecimal] =
    Decoder.instance { av =>
      Option(av.n())
        .toRight(DecoderFailure.invalidTypeFailure(DynamoDbType.N))
        .flatMap(n =>
          Either.catchNonFatal(BigDecimal(n)).leftMap(e =>
            DecoderFailure(e.getMessage, e.some)))
    }

  implicit val dynamoDecoderForShort: Decoder[Short] =
    Decoder.instance { av =>
      Option(av.n())
        .toRight(DecoderFailure.invalidTypeFailure(DynamoDbType.N))
        .flatMap(n =>
          Either.catchNonFatal(n.toShort).leftMap(e =>
            DecoderFailure(e.getMessage, e.some)))
    }

  implicit val dynamoDecoderForByte: Decoder[Byte] =
    Decoder.instance { av =>
      Option(av.n())
        .toRight(DecoderFailure.invalidTypeFailure(DynamoDbType.N))
        .flatMap(n =>
          Either.catchNonFatal(n.toByte).leftMap(e =>
            DecoderFailure(e.getMessage, e.some)))
    }

  implicit val dynamoDecoderForInt: Decoder[Int] =
    Decoder.instance { av =>
      Option(av.n())
        .toRight(DecoderFailure.invalidTypeFailure(DynamoDbType.N))
        .flatMap(n =>
          Either.catchNonFatal(n.toInt).leftMap(e =>
            DecoderFailure(e.getMessage, e.some)))
    }

  implicit val dynamoDecoderForInstant: Decoder[Instant] =
    dynamoDecoderForLong.flatMap {
      ms =>
        Either
          .catchNonFatal(Instant.ofEpochMilli(ms))
          .fold(
            e => Decoder.failed(DecoderFailure(e.getMessage, e.some)),
            ok => Decoder.const(ok)
          )
    }

  implicit def dynamoDecoderForMap[A: Decoder]: Decoder[Map[String, A]] =
    Decoder.instance {
      av =>
        if (av.hasM) {
          av.m()
            .asScala
            .map {
              case (k, v) =>
                (k, Decoder[A].read(v))
            }
            .foldLeft(Map.empty[String, A].asRight[DecoderFailure]) {
              (fs, fx) =>
                for {
                  s <- fs
                  x <- fx._2
                } yield s + (fx._1 -> x)
            }
        } else {
          DecoderFailure.invalidTypeFailure(DynamoDbType.M).asLeft[Map[
            String,
            A
          ]]
        }
    }
}
