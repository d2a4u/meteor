package meteor

import java.util.concurrent.{CompletableFuture, CompletionException}

import cats.effect.{Concurrent, Sync}
import cats.implicits._
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import meteor.codec._

trait syntax {

  implicit class AsAttributeValue[A: Encoder](a: A) {
    def asAttributeValue: AttributeValue = Encoder[A].write(a)
  }

  implicit class RichAttributeValue(av: AttributeValue) {

    def as[A: Decoder]: Either[DecoderFailure, A] = Decoder[A].read(av)

    def get(key: String): Either[DecoderFailure, AttributeValue] =
      if (av.hasM()) {
        av.m().getOrDefault(
          key,
          AttributeValue.builder().nul(true).build()
        ).asRight[DecoderFailure]
      } else {
        DecoderFailure("av is not an M").asLeft[AttributeValue]
      }

    def getAs[A: Decoder](key: String): Either[DecoderFailure, A] =
      get(key).flatMap(_.as[A])
  }

}
