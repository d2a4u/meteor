package meteor

import cats.implicits._
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import meteor.codec._
import meteor.errors.DecoderError

/** Utility to help writing [[meteor.codec.Codec]], [[meteor.codec.Encoder]] and [[meteor.codec.Decoder]].
  *
  * Examples:
  * {{{
  * import meteor.syntax._
  * import meteor.codec._
  *
  * case class Author(
  *   names: String,
  *   age: Int
  * )
  *
  * case class Book(
  *   name: String,
  *   author: Author,
  *   coAuthor: Option[Author]
  * )
  *
  * implicit val encoderForAuthor: Encoder[Author] = Encoder.instance { obj =>
  *   Map(
  *     "names" -> obj.names.asAttributeValue,
  *     "age" -> obj.age.asAttributeValue
  *   ).asAttributeValue
  * }
  *
  * implicit val encoderForBook: Encoder[Book] = Encoder.instance { obj =>
  *   Map(
  *     "name" -> obj.names.asAttributeValue,
  *     "author" -> obj.age.asAttributeValue,
  *     "coAuthor" -> obj.age.asAttributeValue,
  *   ).asAttributeValue
  * }
  *
  * implicit val decoderForAuthor: Decoder[Author] = Decoder.instance { av =>
  *   for {
  *     names <- av.getAs[String]("names")
  *     age <- av.getAs[Int]("age")
  *   } yield Author(names, age)
  * }
  *
  * implicit val decoderForBook: Decoder[Book] = Decoder.instance { av =>
  *   for {
  *     name <- av.getAs[String]("name")
  *     author <- av.getAs[Author]("author")
  *     coAuthor <- av.getOpt[Author]("coAuthor")
  *   } yield Book(name, author, coAuthor)
  * }
  * }}}
  */
trait syntax {

  /** Provide extension method(s) to write to a Java AttributeValue
    */
  implicit class RichWriteAttributeValue[A: Encoder](a: A) {

    /** Write value of type A to an AttributeValue given an `meteor.codec.Encoder` of A in scope.
      */
    def asAttributeValue: AttributeValue = Encoder[A].write(a)
  }

  /** Provide extension method(s) to read from a Java AttributeValue
    */
  implicit class RichReadAttributeValue(av: AttributeValue) {

    /** Attempt to read the AttributeValue as a value of type A or DecoderError
      *
      * Note: if the value of the AttributeValue is nullable, use `asOpt` instead.
      *
      * @return either a value of type A or DecoderError
      */
    def as[A: Decoder]: Either[DecoderError, A] = Decoder[A].read(av)

    /** Attempt to read the AttributeValue as an optional value of type A or DecoderError.
      * This should be used when AttributeValue can be NULL.
      *
      * @return either an optional value of type A or DecoderError
      */
    def asOpt[A: Decoder]: Either[DecoderError, Option[A]] =
      if (Option(av.nul()).exists(_.booleanValue())) None.asRight
      else av.as[A].map(_.some)

    /** Attempt to treat the AttributeValue as of type M, retrieve value by a given key as an
      * AttributeValue.
      *
      * @param key the key whose associated value is to be returned
      * @return either a value as AttributeValue type or DecoderError
      */
    def get(key: String): Either[DecoderError, AttributeValue] =
      if (av.hasM) {
        Option(av.m().get(key)).fold(
          DecoderError.missingKeyFailure(key).asLeft[AttributeValue]
        )(_.asRight[DecoderError])
      } else {
        DecoderError.invalidTypeFailure(DynamoDbType.M).asLeft[AttributeValue]
      }

    /** Attempt to treat the AttributeValue as of type M, retrieve AttributeValue that associated to
      * a given key and attempt reading it to a value of type A.
      *
      * Note: if the returning AttributeValue is nullable, use `getOpt` instead.
      *
      * @param key the key whose associated value is to be returned
      * @return either a value of type A type or DecoderError
      */
    def getAs[A: Decoder](key: String): Either[DecoderError, A] =
      get(key).flatMap(_.as[A])

    /** Attempt to treat the AttributeValue as of type M, retrieve AttributeValue that associated to
      * a given key and attempt reading it to an optional value of type A.
      * This should be used when the returning AttributeValue can be NULL.
      *
      * @param key the key whose associated value is to be returned
      * @return either a value of type A type or DecoderError
      */
    def getOpt[A: Decoder](key: String): Either[DecoderError, Option[A]] =
      if (av.hasM) {
        Option(av.m().get(key)).fold(none[A].asRight[DecoderError])(_.asOpt[A])
      } else {
        DecoderError.invalidTypeFailure(DynamoDbType.M).asLeft[Option[A]]
      }
  }
}

object syntax extends syntax
