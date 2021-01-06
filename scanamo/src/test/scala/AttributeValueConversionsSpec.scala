package meteor
package scanamo
package formats

import meteor.scanamo.formats.conversions._
import com.amazonaws.services.dynamodbv2.model.{
  AttributeValue => AttributeValueV1
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue => AttributeValueV2
}

import scala.jdk.CollectionConverters._

class AttributeValueConversionsSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  behavior.of("AttributeValue")

  it should "round trip convertible between v1 and v2 for Boolean" in forAll {
    bool: Boolean =>
      val v1 = new AttributeValueV1().withBOOL(bool)
      val v2 = AttributeValueV2.builder().bool(bool).build()
      roundTripEquals(v1, v2) shouldBe true
  }

  it should "round trip convertible between v1 and v2 for String" in forAll {
    str: String =>
      val v1 = new AttributeValueV1().withS(str)
      val v2 = AttributeValueV2.builder().s(str).build()
      roundTripEquals(v1, v2) shouldBe true
  }

  it should "round trip convertible between v1 and v2 for Short" in forAll {
    num: Short =>
      val v1 = new AttributeValueV1().withN(num.toString)
      val v2 = AttributeValueV2.builder().n(num.toString).build()
      roundTripEquals(v1, v2) shouldBe true
  }

  it should "round trip convertible between v1 and v2 for Number" in forAll {
    (
      int: Int,
      double: Double,
      long: Long,
      bigInt: BigInt,
      bigDec: BigDecimal,
      float: Float
    ) =>
      val intV1 = new AttributeValueV1().withN(int.toString)
      val intV2 = AttributeValueV2.builder().n(int.toString).build()

      val doubleV1 = new AttributeValueV1().withN(double.toString)
      val doubleV2 = AttributeValueV2.builder().n(double.toString).build()

      val longV1 = new AttributeValueV1().withN(long.toString)
      val longV2 = AttributeValueV2.builder().n(long.toString).build()

      val bigIntV1 = new AttributeValueV1().withN(bigInt.toString)
      val bigIntV2 = AttributeValueV2.builder().n(bigInt.toString).build()

      val bigDecV1 = new AttributeValueV1().withN(bigDec.toString)
      val bigDecV2 = AttributeValueV2.builder().n(bigDec.toString).build()

      val floatV1 = new AttributeValueV1().withN(float.toString)
      val floatV2 = AttributeValueV2.builder().n(float.toString).build()

      roundTripEquals(intV1, intV2) && roundTripEquals(
        intV1,
        intV2
      ) && roundTripEquals(doubleV1, doubleV2) && roundTripEquals(
        doubleV1,
        doubleV2
      ) && roundTripEquals(longV1, longV2) && roundTripEquals(
        longV1,
        longV2
      ) && roundTripEquals(bigIntV1, bigIntV2) && roundTripEquals(
        bigIntV1,
        bigIntV2
      ) && roundTripEquals(bigDecV1, bigDecV2) && roundTripEquals(
        bigDecV1,
        bigDecV2
      ) && roundTripEquals(floatV1, floatV2) && roundTripEquals(
        floatV1,
        floatV2
      ) shouldBe true
  }

  it should "round trip convertible between v1 and v2 for String Set" in forAll {
    strs: List[String] =>
      val v1 = new AttributeValueV1().withSS(strs: _*)
      val v2 = AttributeValueV2.builder().ss(strs: _*).build()
      roundTripEquals(v1, v2) shouldBe true
  }

  it should "round trip convertible between v1 and v2 for Short Set" in forAll {
    short: List[Short] =>
      val v1 = new AttributeValueV1().withNS(short.map(_.toString): _*)
      val v2 = AttributeValueV2.builder().ns(short.map(_.toString): _*).build()
      roundTripEquals(v1, v2) shouldBe true
  }

  it should "round trip convertible between v1 and v2 for Number Set" in forAll {
    (
      int: List[Int],
      double: List[Double],
      long: List[Long],
      bigInt: List[BigInt],
      bigDec: List[BigDecimal],
      float: List[Float]
    ) =>
      val intV1 = new AttributeValueV1().withNS(int.map(_.toString): _*)
      val intV2 = AttributeValueV2.builder().ns(int.map(_.toString): _*).build()

      val doubleV1 = new AttributeValueV1().withNS(double.map(_.toString): _*)
      val doubleV2 =
        AttributeValueV2.builder().ns(double.map(_.toString): _*).build()

      val longV1 = new AttributeValueV1().withNS(long.map(_.toString): _*)
      val longV2 =
        AttributeValueV2.builder().ns(long.map(_.toString): _*).build()

      val bigIntV1 = new AttributeValueV1().withNS(bigInt.map(_.toString): _*)
      val bigIntV2 =
        AttributeValueV2.builder().ns(bigInt.map(_.toString): _*).build()

      val bigDecV1 = new AttributeValueV1().withNS(bigDec.map(_.toString): _*)
      val bigDecV2 =
        AttributeValueV2.builder().ns(bigDec.map(_.toString): _*).build()

      val floatV1 = new AttributeValueV1().withNS(float.map(_.toString): _*)
      val floatV2 =
        AttributeValueV2.builder().ns(float.map(_.toString): _*).build()

      roundTripEquals(intV1, intV2) && roundTripEquals(
        intV1,
        intV2
      ) && roundTripEquals(doubleV1, doubleV2) && roundTripEquals(
        doubleV1,
        doubleV2
      ) && roundTripEquals(longV1, longV2) && roundTripEquals(
        longV1,
        longV2
      ) && roundTripEquals(bigIntV1, bigIntV2) && roundTripEquals(
        bigIntV1,
        bigIntV2
      ) && roundTripEquals(bigDecV1, bigDecV2) && roundTripEquals(
        bigDecV1,
        bigDecV2
      ) && roundTripEquals(floatV1, floatV2) && roundTripEquals(
        floatV1,
        floatV2
      ) shouldBe true
  }

  it should "round trip convertible between v1 and v2 for Null" in {
    val v1 = new AttributeValueV1().withNULL(true)
    val v2 = AttributeValueV2.builder().nul(true).build()
    roundTripEquals(v1, v2) shouldBe true
  }

  it should "round trip convertible between v1 and v2 for List of Boolean" in forAll {
    data: List[Boolean] =>
      val dataV1 = data.map(new AttributeValueV1().withBOOL(_))
      val dataV2 = data.map(AttributeValueV2.builder().bool(_).build())
      val v1 = new AttributeValueV1().withL(dataV1: _*)
      val v2 = AttributeValueV2.builder().l(dataV2: _*).build()
      roundTripEquals(v1, v2) shouldBe true
  }

  it should "round trip convertible between v1 and v2 for List of String" in forAll {
    data: List[String] =>
      val dataV1 = data.map(new AttributeValueV1().withS(_))
      val dataV2 = data.map(AttributeValueV2.builder().s(_).build())
      val v1 = new AttributeValueV1().withL(dataV1: _*)
      val v2 = AttributeValueV2.builder().l(dataV2: _*).build()
      roundTripEquals(v1, v2) shouldBe true
  }

  it should "round trip convertible between v1 and v2 for List of Map of Boolean" in forAll {
    data: List[Map[String, Boolean]] =>
      val dataV1 = data.map { d =>
        val map = d.map {
          case (k, v) =>
            k -> new AttributeValueV1().withBOOL(v)
        }.asJava
        new AttributeValueV1().withM(map)
      }
      val dataV2 = data.map { d =>
        val map = d.map {
          case (k, v) =>
            k -> AttributeValueV2.builder().bool(v).build()
        }.asJava
        AttributeValueV2.builder().m(map).build()
      }
      val v1 = new AttributeValueV1().withL(dataV1: _*)
      val v2 = AttributeValueV2.builder().l(dataV2: _*).build()
      roundTripEquals(v1, v2) shouldBe true
  }

  it should "round trip convertible between v1 and v2 for List of Map of String" in forAll {
    data: List[Map[String, String]] =>
      val dataV1 = data.map { d =>
        val map = d.map {
          case (k, v) =>
            k -> new AttributeValueV1().withS(v)
        }.asJava
        new AttributeValueV1().withM(map)
      }
      val dataV2 = data.map { d =>
        val map = d.map {
          case (k, v) =>
            k -> AttributeValueV2.builder().s(v).build()
        }.asJava
        AttributeValueV2.builder().m(map).build()
      }
      val v1 = new AttributeValueV1().withL(dataV1: _*)
      val v2 = AttributeValueV2.builder().l(dataV2: _*).build()
      roundTripEquals(v1, v2) shouldBe true
  }

  it should "round trip convertible between v1 and v2 for Map of Boolean" in forAll {
    data: Map[String, Boolean] =>
      val dataV1 = data.map {
        case (k, v) =>
          k -> new AttributeValueV1().withBOOL(v)
      }.asJava
      val dataV2 = data.map {
        case (k, v) =>
          k -> AttributeValueV2.builder().bool(v).build()
      }.asJava
      val v1 = new AttributeValueV1().withM(dataV1)
      val v2 = AttributeValueV2.builder().m(dataV2).build()
      roundTripEquals(v1, v2) shouldBe true
  }

  it should "round trip convertible between v1 and v2 for Map of String" in forAll {
    data: Map[String, String] =>
      val dataV1 = data.map {
        case (k, v) =>
          k -> new AttributeValueV1().withS(v)
      }.asJava
      val dataV2 = data.map {
        case (k, v) =>
          k -> AttributeValueV2.builder().s(v).build()
      }.asJava
      val v1 = new AttributeValueV1().withM(dataV1)
      val v2 = AttributeValueV2.builder().m(dataV2).build()
      roundTripEquals(v1, v2) shouldBe true
  }

  it should "round trip convertible between v1 and v2 for Map of List of Boolean" in forAll {
    data: Map[String, List[Boolean]] =>
      val dataV1 = data.map {
        case (k, v) =>
          k -> new AttributeValueV1().withL(v.map(b =>
            new AttributeValueV1().withBOOL(b)): _*)
      }.asJava
      val dataV2 = data.map {
        case (k, v) =>
          k -> AttributeValueV2.builder().l(v.map(b =>
            AttributeValueV2.builder().bool(b).build()): _*).build()
      }.asJava
      val v1 = new AttributeValueV1().withM(dataV1)
      val v2 = AttributeValueV2.builder().m(dataV2).build()
      roundTripEquals(v1, v2) shouldBe true
  }

  it should "round trip convertible between v1 and v2 for Map of List of String" in forAll {
    data: Map[String, List[String]] =>
      val dataV1 = data.map {
        case (k, v) =>
          k -> new AttributeValueV1().withL(v.map(b =>
            new AttributeValueV1().withS(b)): _*)
      }.asJava
      val dataV2 = data.map {
        case (k, v) =>
          k -> AttributeValueV2.builder().l(v.map(b =>
            AttributeValueV2.builder().s(b).build()): _*).build()
      }.asJava
      val v1 = new AttributeValueV1().withM(dataV1)
      val v2 = AttributeValueV2.builder().m(dataV2).build()
      roundTripEquals(v1, v2) shouldBe true
  }

  def roundTripEquals(v1: AttributeValueV1, v2: AttributeValueV2): Boolean = {
    val convertedV2 = sdk1ToSdk2AttributeValue(v1)
    val convertedV1 = sdk2ToSdk1AttributeValue(v2)
    convertedV2.equals(v2) && convertedV1.equals(v1)
  }
}
