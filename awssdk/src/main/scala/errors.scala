package meteor

object errors {
  sealed abstract class DynamoError extends Exception

  case object InvalidExpression extends DynamoError {
    override def getMessage: String =
      "The expression is invalid"
  }
}
