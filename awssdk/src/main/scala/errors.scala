package meteor

import cats.implicits._

import software.amazon.awssdk.services.dynamodb.model.TableStatus

object errors {
  sealed abstract class DynamoError extends Exception

  case class DecoderError(message: String, cause: Option[Throwable] = None)
      extends DynamoError {
    override def getMessage: String = message
  }

  case class EncoderError(message: String) extends DynamoError {
    override def getMessage: String = message
  }

  object DecoderError {
    def invalidTypeFailure(t: DynamoDbType): DecoderError =
      DecoderError(s"The AttributeValue must be of type ${t.show}")

    def missingKeyFailure(key: String): DecoderError =
      DecoderError(s"The AttributeValue is a map but $key key doesn't exists")
  }

  object EncoderError {
    def invalidTypeFailure(t: DynamoDbType): EncoderError =
      EncoderError(s"The AttributeValue must be of type ${t.show}")

    val invalidKeyTypeFailure: EncoderError =
      EncoderError(s"The AttributeValue for key must be of type B, S or N")

    def missingKeyFailure: EncoderError =
      EncoderError(
        s"The AttributeValue is a map but does not contain index key attribute(s)"
      )
  }

  case object InvalidExpression extends DynamoError {
    override def getMessage: String =
      "The expression is invalid"
  }

  case class InvalidKeyType[K](k: K) extends DynamoError {
    override def getMessage: String =
      s"Key of type ${k.getClass.getTypeName} is invalid, expect either N, S or B"
  }

  case class UnexpectedTableStatus(
    tableName: String,
    status: TableStatus,
    expects: Set[TableStatus]
  ) extends DynamoError {
    override def getMessage: String =
      s"$tableName table's status is ${status.toString}, expect ${expects.mkString(", ")}"
  }

  case class ConditionalCheckFailed(msg: String) extends DynamoError {
    override def getMessage: String = msg
  }

  case class UnsupportedArgument(msg: String) extends DynamoError {
    override def getMessage: String = msg
  }
}
