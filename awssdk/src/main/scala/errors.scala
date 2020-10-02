package meteor

object errors {
  sealed abstract class DynamoError extends Exception

  case object InvalidCondition extends DynamoError {
    override def getMessage: String =
      s"Could not render query expression correctly"
  }
}
