package meteor.dynosaur
package formats

import cats.syntax.all._
import dynosaur.Schema
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import meteor.codec.Codec

case class Book(title: String, author: String)

class ConversionsUsageSpec extends AnyFlatSpec with Matchers {
  it should "support implicit usage" in {
    implicit val bookSchema: Schema[Book] = Schema.record { field =>
      (
        field("title", _.title),
        field("author", _.author)
      ).mapN(Book.apply)
    }

    import conversions._

    implicitly[Codec[Book]] should not be null
  }
}
