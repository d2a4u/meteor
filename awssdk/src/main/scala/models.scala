package meteor

import cats._
import cats.implicits._
import meteor.codec.Encoder
import meteor.syntax._
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  ScalarAttributeType
}

import java.util
import scala.jdk.CollectionConverters._

case class Key(
  name: String,
  attributeType: ScalarAttributeType
)

case class Table(
  name: String,
  partitionKey: Key,
  sortKey: Option[Key]
) {
  def keys[P: Encoder, S: Encoder](
    partitionKeyValue: P,
    sortKeyValue: Option[S]
  ): util.Map[String, AttributeValue] = {
    val partitionK =
      Map(partitionKey.name -> partitionKeyValue.asAttributeValue)

    val optSortK = for {
      key <- sortKey
      value <- sortKeyValue
    } yield Map(key.name -> value.asAttributeValue)

    optSortK.fold(partitionK) { sortK =>
      partitionK ++ sortK
    }.asJava
  }
}

case class Index(name: String) extends AnyVal

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
}

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

case class Query[P: Encoder, S: Encoder](
  partitionKey: P,
  sortKeyQuery: SortKeyQuery[S],
  filter: Expression
) {
  def keyCondition(table: Table): Expression = {

    def mkSortKeyExpression(sortKeyName: String) =
      sortKeyQuery match {
        case SortKeyQuery.EqualTo(value) =>
          val placeholder = ":t1"
          Expression(
            s"#$sortKeyName EQ $placeholder",
            Map(s"#$sortKeyName" -> sortKeyName),
            Map(placeholder -> value.asAttributeValue)
          ).some

        case SortKeyQuery.LessThan(value) =>
          val placeholder = ":t1"
          Expression(
            s"#$sortKeyName LT $placeholder",
            Map(s"#$sortKeyName" -> sortKeyName),
            Map(placeholder -> value.asAttributeValue)
          ).some

        case SortKeyQuery.LessOrEqualTo(value) =>
          val placeholder = ":t1"
          Expression(
            s"#$sortKeyName LE $placeholder",
            Map(s"#$sortKeyName" -> sortKeyName),
            Map(placeholder -> value.asAttributeValue)
          ).some

        case SortKeyQuery.GreaterThan(value) =>
          val placeholder = ":t1"
          Expression(
            s"#$sortKeyName GT $placeholder",
            Map(s"#$sortKeyName" -> sortKeyName),
            Map(placeholder -> value.asAttributeValue)
          ).some

        case SortKeyQuery.GreaterOrEqualTo(value) =>
          val placeholder = ":t1"
          Expression(
            s"#$sortKeyName GE $placeholder",
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

    val partitionKeyAV = Encoder[P].write(partitionKey)
    val placeholder = ":t0"

    val partitionKeyExpression = Expression(
      s"#${table.partitionKey.name} = $placeholder",
      Map(s"#${table.partitionKey.name}" -> table.partitionKey.name),
      Map(placeholder -> partitionKeyAV)
    )

    val optSortKeyExpression = for {
      sortKey <- table.sortKey
      sortKeyExp <- mkSortKeyExpression(sortKey.name)
    } yield sortKeyExp

    Monoid.maybeCombine(partitionKeyExpression, optSortKeyExpression)
  }
}

object Query {
  def apply[P: Encoder, S: Encoder](
    partitionKey: P
  ): Query[P, S] =
    Query(partitionKey, SortKeyQuery.Empty[S](), Expression.empty)

  def apply[P: Encoder, S: Encoder](
    partitionKey: P,
    sortKeyQuery: SortKeyQuery[S]
  ): Query[P, S] = Query(partitionKey, sortKeyQuery, Expression.empty)

  def apply[P: Encoder, S: Encoder](
    partitionKey: P,
    filter: Expression
  ): Query[P, S] = Query(partitionKey, SortKeyQuery.Empty[S](), filter)
}
