package meteor

import meteor.codec.Encoder
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  KeyType,
  ScalarAttributeType
}

import scala.jdk.CollectionConverters._

case class Key(
  name: String,
  attributeType: ScalarAttributeType
)

case class Table(
  name: String,
  hashKey: Key,
  rangeKey: Option[Key]
)

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
}

case class Query[P: Encoder, S: Encoder](
  partitionKey: P,
  sortKeyQuery: SortKeyQuery[S],
  filter: Expression
) {
  val keyCondition: Expression = {
    val pKey = Encoder[P].write(partitionKey).m().asScala.toList.headOption
    val sKey = sortKeyQuery match {
      case SortKeyQuery.EqualTo(value) =>
        Encoder[S].write(value).m().asScala.toList.headOption.map {
          case (str, attr) =>
            val placeholder = ":t1"
            Expression(
              s"#$str EQ $placeholder",
              Map(s"#$str" -> str),
              Map(placeholder -> attr)
            )
        }

      case SortKeyQuery.LessThan(value) =>
        Encoder[S].write(value).m().asScala.toList.headOption.map {
          case (str, attr) =>
            val placeholder = ":t1"
            Expression(
              s"#$str LT $placeholder",
              Map(s"#$str" -> str),
              Map(placeholder -> attr)
            )
        }

      case SortKeyQuery.LessOrEqualTo(value) =>
        Encoder[S].write(value).m().asScala.toList.headOption.map {
          case (str, attr) =>
            val placeholder = ":t1"
            Expression(
              s"#$str LE $placeholder",
              Map(s"#$str" -> str),
              Map(placeholder -> attr)
            )
        }

      case SortKeyQuery.GreaterThan(value) =>
        Encoder[S].write(value).m().asScala.toList.headOption.map {
          case (str, attr) =>
            val placeholder = ":t1"
            Expression(
              s"#$str GT $placeholder",
              Map(s"#$str" -> str),
              Map(placeholder -> attr)
            )
        }

      case SortKeyQuery.GreaterOrEqualTo(value) =>
        Encoder[S].write(value).m().asScala.toList.headOption.map {
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
          f <- Encoder[S].write(from).m().asScala.toList.headOption
          t <- Encoder[S].write(to).m().asScala.toList.headOption
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
        Encoder[S].write(value).m().asScala.toList.headOption.map {
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
    pKey.fold(Expression.empty) { p =>
      val placeholder = ":t0"
      sKey.fold(Expression(
        s"#${p._1} = $placeholder",
        Map(s"#${p._1}" -> p._1),
        Map(placeholder -> p._2)
      )) { s =>
        Expression(
          s"#${p._1} = $placeholder AND ",
          s.attributeNames ++ Map(s"#${p._1}" -> p._1),
          s.attributeValues ++ Map(placeholder -> p._2)
        )
      }
    }
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
