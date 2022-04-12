package meteor
package scanamo
package formats

import cats.syntax.all._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scanamo.DynamoFormat
import org.scanamo.generic.semiauto._
import meteor.codec.Codec

case class Book(title: String, author: String)

class ConversionsUsageSpec extends AnyFlatSpec with Matchers {
  it should "support implicit usage" in {
    implicit val bookFormat: DynamoFormat[Book] = deriveDynamoFormat

    import conversions._

    implicitly[Codec[Book]] should not be null
  }
}
