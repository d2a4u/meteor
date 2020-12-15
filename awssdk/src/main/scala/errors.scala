package meteor

import software.amazon.awssdk.services.dynamodb.model.TableStatus

object errors {
  sealed abstract class DynamoError extends Exception

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
