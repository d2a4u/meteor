package meteor

import cats.implicits._

import meteor.DynamoDbType
import software.amazon.awssdk.services.dynamodb.model.TableStatus

object errors {
  sealed abstract class DynamoError extends Exception

  case class DecoderError(message: String, cause: Option[Throwable] = None)
      extends DynamoError

  object DecoderError {
    def invalidTypeFailure(t: DynamoDbType): DecoderError =
      DecoderError(s"The AttributeValue must be of type ${t.show}")

    val nullValue: DecoderError =
      DecoderError("Returned value is null")
  }

  case object InvalidExpression extends DynamoError {
    override def getMessage: String =
      "The expression is invalid"
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
}
