package meteor
package codec

import cats.Show

object primitives {
  trait DynamoDbType

  object DynamoDbType {
    case object BOOL extends DynamoDbType //boolean
    case object B extends DynamoDbType //binary
    case object BS extends DynamoDbType //binary set
    case object L extends DynamoDbType //list
    case object M extends DynamoDbType //map
    case object N extends DynamoDbType //number
    case object NS extends DynamoDbType //number set
    case object NULL extends DynamoDbType //null
    case object S extends DynamoDbType //string
    case object SS extends DynamoDbType //string set

    implicit val dynamoDbTypeShow: Show[DynamoDbType] =
      Show.fromToString[DynamoDbType]
  }
}
