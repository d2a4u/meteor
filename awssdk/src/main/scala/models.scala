package meteor

import meteor.codec.Encoder
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import scala.jdk.CollectionConverters._

sealed trait PutItemReturnValue
object PutItemReturnValue {
  case object None extends PutItemReturnValue
  case object AllOld extends PutItemReturnValue
}

case class Table(name: String) extends AnyVal

case class Index(name: String) extends AnyVal

case object EmptySortKey {
  implicit val emptySortKeyEncoder: Encoder[EmptySortKey.type] =
    Encoder.instance[EmptySortKey.type] { _ =>
      AttributeValue.builder().m(
        Map.empty[String, AttributeValue].asJava
      ).build()
    }
}

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

case class Condition(
  expression: String,
  attributes: Map[String, AttributeValue]
)

case class Query[P: Encoder, S: Encoder](
  partitionKey: P,
  sortKeyQuery: SortKeyQuery[S]
) {
  val condition: Option[Condition] = {
    val pKey = Encoder[P].write(partitionKey).m().asScala.toList.headOption
    val sKey = sortKeyQuery match {
      case SortKeyQuery.EqualTo(value) =>
        Encoder[S].write(value).m().asScala.toList.headOption.map {
          case (str, attr) =>
            val placeholder = ":t1"
            Condition(s"$str EQ $placeholder", Map(placeholder -> attr))
        }

      case SortKeyQuery.LessThan(value) =>
        Encoder[S].write(value).m().asScala.toList.headOption.map {
          case (str, attr) =>
            val placeholder = ":t1"
            Condition(s"$str LT $placeholder", Map(placeholder -> attr))
        }

      case SortKeyQuery.LessOrEqualTo(value) =>
        Encoder[S].write(value).m().asScala.toList.headOption.map {
          case (str, attr) =>
            val placeholder = ":t1"
            Condition(s"$str LE $placeholder", Map(placeholder -> attr))
        }

      case SortKeyQuery.GreaterThan(value) =>
        Encoder[S].write(value).m().asScala.toList.headOption.map {
          case (str, attr) =>
            val placeholder = ":t1"
            Condition(s"$str GT $placeholder", Map(placeholder -> attr))
        }

      case SortKeyQuery.GreaterOrEqualTo(value) =>
        Encoder[S].write(value).m().asScala.toList.headOption.map {
          case (str, attr) =>
            val placeholder = ":t1"
            Condition(s"$str GE $placeholder", Map(placeholder -> attr))
        }

      case SortKeyQuery.Between(from, to) =>
        for {
          f <- Encoder[S].write(from).m().asScala.toList.headOption
          t <- Encoder[S].write(to).m().asScala.toList.headOption
        } yield {
          val placeholder1 = ":t1"
          val placeholder2 = ":t2"
          Condition(
            s"${f._1} BETWEEN $placeholder1 AND $placeholder2",
            Map(placeholder1 -> f._2, placeholder2 -> t._2)
          )
        }

      case _ =>
        None
    }
    pKey.map { p =>
      val placeholder = ":t0"
      sKey.fold(Condition(
        s"${p._1} = $placeholder",
        Map(placeholder -> p._2)
      )) { s =>
        Condition(
          s"${p._1} = $placeholder AND ",
          s.attributes ++ Map(placeholder -> p._2)
        )
      }
    }
  }
}
