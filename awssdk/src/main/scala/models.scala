package meteor

import cats._
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
          Encoder[(String, S)].write(
            sortKeyName -> value
          ).m().asScala.toList.headOption.map {
            case (str, attr) =>
              val placeholder = ":t1"
              Expression(
                s"#$str EQ $placeholder",
                Map(s"#$str" -> str),
                Map(placeholder -> attr)
              )
          }

        case SortKeyQuery.LessThan(value) =>
          Encoder[(String, S)].write(
            sortKeyName -> value
          ).m().asScala.toList.headOption.map {
            case (str, attr) =>
              val placeholder = ":t1"
              Expression(
                s"#$str LT $placeholder",
                Map(s"#$str" -> str),
                Map(placeholder -> attr)
              )
          }

        case SortKeyQuery.LessOrEqualTo(value) =>
          Encoder[(String, S)].write(
            sortKeyName -> value
          ).m().asScala.toList.headOption.map {
            case (str, attr) =>
              val placeholder = ":t1"
              Expression(
                s"#$str LE $placeholder",
                Map(s"#$str" -> str),
                Map(placeholder -> attr)
              )
          }

        case SortKeyQuery.GreaterThan(value) =>
          Encoder[(String, S)].write(
            sortKeyName -> value
          ).m().asScala.toList.headOption.map {
            case (str, attr) =>
              val placeholder = ":t1"
              Expression(
                s"#$str GT $placeholder",
                Map(s"#$str" -> str),
                Map(placeholder -> attr)
              )
          }

        case SortKeyQuery.GreaterOrEqualTo(value) =>
          Encoder[(String, S)].write(
            sortKeyName -> value
          ).m().asScala.toList.headOption.map {
            case (str, attr) =>
              val placeholder = ":t1"
              Expression(
                s"#$str GE $placeholder",
                Map(s"#$str" -> str),
                Map(placeholder -> attr)
              )
          }

        case SortKeyQuery.Between(from, to) =>
          for {
            f <- Encoder[(String, S)].write(
              sortKeyName -> from
            ).m().asScala.toList.headOption
            t <- Encoder[(String, S)].write(
              sortKeyName -> to
            ).m().asScala.toList.headOption
          } yield {
            val placeholder1 = ":t1"
            val placeholder2 = ":t2"
            Expression(
              s"#${f._1} BETWEEN $placeholder1 AND $placeholder2",
              Map(s"#${f._1}" -> f._1),
              Map(placeholder1 -> f._2, placeholder2 -> t._2)
            )
          }

        case SortKeyQuery.BeginsWith(value) =>
          Encoder[(String, S)].write(
            sortKeyName -> value
          ).m().asScala.toList.headOption.map {
            case (str, attr) =>
              val placeholder = ":t1"
              Expression(
                s"begins_with(#$str, $placeholder)",
                Map(s"#$str" -> str),
                Map(placeholder -> attr)
              )
          }

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
