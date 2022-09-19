package meteor

import cats._
import cats.implicits._
import meteor.codec.Encoder
import meteor.errors.EncoderError
import meteor.implicits._
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  ScalarAttributeType
}

import java.util
import java.util.{HashMap => jHashMap}
import scala.jdk.CollectionConverters._

/** Key's definition, a representation of DynamoDB's key.
  *
  * @param attributeName attribute's name
  * @param attributeType attribute's type
  * @tparam K key's type
  */
case class KeyDef[K](
  attributeName: String,
  attributeType: DynamoDbType
) {
  def mkKey[F[_]: MonadThrow](k: K)(implicit
  encoder: Encoder[K]): F[java.util.Map[String, AttributeValue]] = {
    val av = k.asAttributeValue
    val valid = Map(attributeName -> av).asJava.pure[F]
    if (attributeType == DynamoDbType.B && av.b() != null) {
      valid
    } else if (attributeType == DynamoDbType.S && av.s() != null) {
      valid
    } else if (attributeType == DynamoDbType.N && av.n() != null) {
      valid
    } else {
      EncoderError.invalidKeyTypeFailure.raiseError[F, java.util.Map[
        String,
        AttributeValue
      ]]
    }
  }

  val value: (String, DynamoDbType) = attributeName -> attributeType
}

object KeyDef {
  val nothing: KeyDef[Nothing] = KeyDef(null, DynamoDbType.NULL)
}

private[meteor] sealed trait Index[P] {
  def partitionKeyDef: KeyDef[P]

  def tableName: String
  def containKey(av: java.util.Map[String, AttributeValue])
    : Option[java.util.Map[String, AttributeValue]]
  def extractKey[F[_]: MonadThrow, T: Encoder](t: T)
    : F[java.util.Map[String, AttributeValue]] = {
    val av = t.asAttributeValue
    if (av.hasM) {
      val m = av.m()
      MonadError[F, Throwable].fromOption(
        containKey(m),
        EncoderError.missingKeyFailure
      )
    } else {
      EncoderError.invalidTypeFailure(DynamoDbType.M).raiseError[
        F,
        java.util.Map[
          String,
          AttributeValue
        ]
      ]
    }
  }
}

private[meteor] sealed trait PartitionKeyIndex[P] extends Index[P] {
  def containKey(av: util.Map[String, AttributeValue])
    : Option[java.util.Map[String, AttributeValue]] = {
    av.containsKey(partitionKeyDef.attributeName).guard[Option].as {
      val m = new jHashMap[String, AttributeValue]()
      m.put(
        partitionKeyDef.attributeName,
        av.get(partitionKeyDef.attributeName)
      )
      m
    }
  }

  def mkKey[F[_]: MonadThrow](p: P)(implicit
  encoder: Encoder[P]): F[java.util.Map[String, AttributeValue]] =
    partitionKeyDef.mkKey[F](p)
}

private[meteor] sealed trait CompositeKeysIndex[P, S] extends Index[P] {
  def sortKeyDef: KeyDef[S]

  def containKey(av: util.Map[String, AttributeValue])
    : Option[java.util.Map[String, AttributeValue]] = {
    val cond = av.containsKey(partitionKeyDef.attributeName) && av.containsKey(
      sortKeyDef.attributeName
    )
    cond.guard[Option].as {
      val m = new jHashMap[String, AttributeValue]()
      m.put(
        partitionKeyDef.attributeName,
        av.get(partitionKeyDef.attributeName)
      )
      m.put(
        sortKeyDef.attributeName,
        av.get(sortKeyDef.attributeName)
      )
      m
    }
  }

  def mkKey[F[_]: MonadThrow](
    p: P,
    s: S
  )(
    implicit encoderP: Encoder[P],
    encoderS: Encoder[S]
  ): F[java.util.Map[String, AttributeValue]] =
    for {
      pk <- partitionKeyDef.mkKey[F](p)
      sk <- sortKeyDef.mkKey[F](s)
    } yield pk ++ sk
}

/** Represent a table which has only partition key
  *
  * @param tableName table's name
  * @param partitionKeyDef partition key's definition
  * @tparam P partition key's type
  */
private[meteor] case class PartitionKeyTable[P](
  tableName: String,
  partitionKeyDef: KeyDef[P]
) extends PartitionKeyIndex[P]

/** Represent a table which has both partition key and sort key
  *
  * @param tableName table's name
  * @param partitionKeyDef partition key's definition
  * @param sortKeyDef sort key's definition
  * @tparam P partition key's type
  * @tparam S sort key's type
  */
private[meteor] case class CompositeKeysTable[P, S](
  tableName: String,
  partitionKeyDef: KeyDef[P],
  sortKeyDef: KeyDef[S]
) extends CompositeKeysIndex[P, S]

/** Represent a secondary index which has only partition key
  *
  * @param tableName table's name
  * @param indexName secondary index's name
  * @param partitionKeyDef partition key's definition
  * @tparam P partition key's type
  */
private[meteor] case class PartitionKeySecondaryIndex[P](
  tableName: String,
  indexName: String,
  partitionKeyDef: KeyDef[P]
) extends PartitionKeyIndex[P]

/** Represent a secondary index which has both partition key and sort key
  *
  * @param tableName table's name
  * @param indexName index's name
  * @param partitionKeyDef partition key's definition
  * @param sortKeyDef sort key's  definition
  * @tparam P partition key's type
  * @tparam S sort key's type
  */
private[meteor] case class CompositeKeysSecondaryIndex[P, S](
  tableName: String,
  indexName: String,
  partitionKeyDef: KeyDef[P],
  sortKeyDef: KeyDef[S]
) extends CompositeKeysIndex[P, S]

/** Represent sort key query which can be used as part of key condition expression for query action:
  * https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-KeyConditionExpression
  */
sealed trait SortKeyQuery[T]
object SortKeyQuery {
  case class Empty[T]() extends SortKeyQuery[T]
  case class EqualTo[T](value: T) extends SortKeyQuery[T]
  case class LessThan[T](value: T) extends SortKeyQuery[T]
  case class LessOrEqualTo[T](value: T) extends SortKeyQuery[T]
  case class GreaterThan[T](value: T) extends SortKeyQuery[T]
  case class GreaterOrEqualTo[T](value: T) extends SortKeyQuery[T]
  case class Between[T: Ordering](from: T, to: T) extends SortKeyQuery[T]
  case class BeginsWith[T](value: T) extends SortKeyQuery[T]

  def empty[T]: SortKeyQuery[T] = Empty[T]()
}

/** Abstraction over DynamoDB expressions, this can be key condition expression, update expression, projection expression etc..
  * https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.html.
  *
  * It is recommended to avoid DynamoDB's reserved words in expression string by providing expression as
  * raw String with attribute names and values as placeholders only. Attribute names and values can then
  * be replaced separately via `attributeNames` and `attributeValues` maps.
  *
  * Example:
  * {{{
  * import meteor.Expression
  * import meteor.syntax._
  *
  * Expression(
  *   "#b = :my_bool and #i > :my_int",
  *   Map("#b" -> "my_bool_attribute_name", "#i" -> "my_int_attribute_name"),
  *   Map(
  *     ":my_bool" -> true.asAttributeValue,
  *     ":my_int" -> 0.asAttributeValue
  *   )
  * )
  * }}}
  * @param expression expression as raw String
  * @param attributeNames a map of attribute name placeholders in the raw String above to the actual attribute names in the table
  * @param attributeValues a map of attribute value placeholders in the raw String above to the actual attribute values
  */
case class Expression(
  expression: String,
  attributeNames: Map[String, String],
  attributeValues: Map[String, AttributeValue]
) {
  val isEmpty: Boolean = this.expression.isEmpty
  val nonEmpty: Boolean = !isEmpty
}

object Expression {
  val empty: Expression =
    Expression("", Map.empty[String, String], Map.empty[String, AttributeValue])

  def apply(expression: String): Expression =
    Expression(expression, Map.empty, Map.empty)

  implicit val monoidOfExpression: Monoid[Expression] = Monoid.instance(
    Expression.empty,
    { (left, right) =>
      if (left.isEmpty) {
        right
      } else if (right.isEmpty) {
        left
      } else {
        Expression(
          left.expression + " AND " + right.expression,
          left.attributeNames ++ right.attributeNames,
          left.attributeValues ++ right.attributeValues
        )
      }
    }
  )
}

/** Represent a DynamoDB's query where a partition key value is required,
  * `sortKeyQuery` and `filter` are optional.
  *
  * Examples:
  * {{{
  * // query for an item where partition key == "some-partition-key", sort key == "some-sort-key" but only
  * // return a value if the filter's condition is med ("my_bool_attribute_name" == true)
  * val query: Query[String, String] =
  *   Query(
  *     "some-partition-key",
  *     SortKeyQuery.EqualTo("some-sort-key"),
  *     Expression(
  *       "#b = :my_bool",
  *       Map("#b" -> "my_bool_attribute_name"),
  *       Map(
  *         ":my_bool" -> true.asAttributeValue
  *       )
  *     )
  *   )
  *
  * // query for an item where partition key == "some-partition-key", the table doesn't have sort key, only
  * // return a value if the filter's condition is med ("my_bool_attribute_name" == true)
  * val query: Query[String, Nothing] =
  *   Query(
  *     "some-partition-key",
  *     Expression(
  *       "#b = :my_bool",
  *       Map("#b" -> "my_bool_attribute_name"),
  *       Map(
  *         ":my_bool" -> true.asAttributeValue
  *       )
  *     )
  *   )
  * }}}
  * @param partitionKey partition key value
  * @param sortKeyQuery sort key query
  * @param filter filter expression
  * @tparam P partition key's type
  * @tparam S sort key's type (`Nothing` type for table without sort key)
  */
case class Query[P: Encoder, S: Encoder](
  partitionKey: P,
  sortKeyQuery: SortKeyQuery[S],
  filter: Expression
) {
  private[meteor] def keysCondition(index: CompositeKeysIndex[P, S])
    : Expression = {
    val partitionKeyExpression =
      mkPartitionKeyExpression(index.partitionKeyDef.attributeName)

    val optSortKeyExpression = {
      if (index.sortKeyDef == KeyDef.nothing) None
      else mkSortKeyExpression(index.sortKeyDef.attributeName)
    }

    Monoid.maybeCombine(partitionKeyExpression, optSortKeyExpression)
  }

  private[meteor] def keyCondition(table: Index[P]): Expression =
    mkPartitionKeyExpression(table.partitionKeyDef.attributeName)

  private def mkPartitionKeyExpression(partitionKeyAttributeName: String) = {
    val placeholder = ":t0"
    val partitionKeyAV = partitionKey.asAttributeValue
    Expression(
      s"#$partitionKeyAttributeName = $placeholder",
      Map(
        s"#$partitionKeyAttributeName" -> partitionKeyAttributeName
      ),
      Map(placeholder -> partitionKeyAV)
    )
  }

  private def mkSortKeyExpression(sortKeyName: String) = {
    sortKeyQuery match {
      case SortKeyQuery.EqualTo(value) =>
        val placeholder = ":t1"
        Expression(
          s"#$sortKeyName = $placeholder",
          Map(s"#$sortKeyName" -> sortKeyName),
          Map(placeholder -> value.asAttributeValue)
        ).some

      case SortKeyQuery.LessThan(value) =>
        val placeholder = ":t1"
        Expression(
          s"#$sortKeyName < $placeholder",
          Map(s"#$sortKeyName" -> sortKeyName),
          Map(placeholder -> value.asAttributeValue)
        ).some

      case SortKeyQuery.LessOrEqualTo(value) =>
        val placeholder = ":t1"
        Expression(
          s"#$sortKeyName <= $placeholder",
          Map(s"#$sortKeyName" -> sortKeyName),
          Map(placeholder -> value.asAttributeValue)
        ).some

      case SortKeyQuery.GreaterThan(value) =>
        val placeholder = ":t1"
        Expression(
          s"#$sortKeyName > $placeholder",
          Map(s"#$sortKeyName" -> sortKeyName),
          Map(placeholder -> value.asAttributeValue)
        ).some

      case SortKeyQuery.GreaterOrEqualTo(value) =>
        val placeholder = ":t1"
        Expression(
          s"#$sortKeyName >= $placeholder",
          Map(s"#$sortKeyName" -> sortKeyName),
          Map(placeholder -> value.asAttributeValue)
        ).some

      case SortKeyQuery.Between(from, to) =>
        val placeholder1 = ":t1"
        val placeholder2 = ":t2"
        Expression(
          s"#$sortKeyName BETWEEN $placeholder1 AND $placeholder2",
          Map(s"#$sortKeyName" -> sortKeyName),
          Map(
            placeholder1 -> from.asAttributeValue,
            placeholder2 -> to.asAttributeValue
          )
        ).some

      case SortKeyQuery.BeginsWith(value) =>
        val placeholder = ":t1"
        Expression(
          s"begins_with(#$sortKeyName, $placeholder)",
          Map(s"#$sortKeyName" -> sortKeyName),
          Map(placeholder -> value.asAttributeValue)
        ).some

      case _ =>
        None
    }
  }
}

object Query {
  def apply[P: Encoder](
    partitionKey: P
  ): Query[P, Nothing] =
    Query[P](
      partitionKey,
      Expression.empty
    )

  def apply[P: Encoder, S: Encoder](
    partitionKey: P,
    sortKeyQuery: SortKeyQuery[S]
  ): Query[P, S] = Query(partitionKey, sortKeyQuery, Expression.empty)

  /** Create a Query where the table doesn't have a sort key.
    *
    * @param partitionKey partition key
    * @param filter filter expression
    * @return a query
    */
  def apply[P: Encoder](
    partitionKey: P,
    filter: Expression
  ): Query[P, Nothing] =
    Query[P, Nothing](partitionKey, SortKeyQuery.empty[Nothing], filter)
}

/** Represent DynamoDB primitive data types, including BOOL, B, BS, L, M, N, NS, NULL, S and SS.
  */
trait DynamoDbType {
  def toScalarAttributeType: ScalarAttributeType =
    this match {
      case DynamoDbType.B =>
        ScalarAttributeType.B

      case DynamoDbType.S =>
        ScalarAttributeType.S

      case DynamoDbType.N =>
        ScalarAttributeType.N

      case _ =>
        ScalarAttributeType.UNKNOWN_TO_SDK_VERSION
    }
}

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
