package meteor
package scanamo
package formats

import com.amazonaws.services.dynamodbv2.model.{
  AttributeValue => AttributeValueV1
}
import meteor.codec.{Decoder, DecoderFailure, Encoder}
import org.scanamo.DynamoFormat
import org.scanamo.error.DynamoReadError
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.core.util.{
  DefaultSdkAutoConstructList,
  DefaultSdkAutoConstructMap
}
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import scala.language.implicitConversions
import scala.jdk.CollectionConverters._

object conversions {

  def sdk2ToSdk1AttributeValue(av: AttributeValue): AttributeValueV1 = {
    if (av.nul() != null) {
      new AttributeValueV1().withNULL(true)
    } else if (av.bool() != null) {
      new AttributeValueV1().withBOOL(av.bool())
    } else if (av.b() != null) {
      new AttributeValueV1().withB(av.b().asByteBuffer())
    } else if (
      av.bs() != null && !av.bs().isInstanceOf[DefaultSdkAutoConstructList[_]]
    ) {
      new AttributeValueV1().withBS(
        av.bs().asScala.map(_.asByteBuffer()).asJava
      )
    } else if (
      av.l() != null && !av.l().isInstanceOf[DefaultSdkAutoConstructList[_]]
    ) {
      new AttributeValueV1().withL(
        av.l().asScala.map(sdk2ToSdk1AttributeValue).asJava
      )
    } else if (
      av.m() != null && !av.m().isInstanceOf[DefaultSdkAutoConstructMap[_, _]]
    ) {
      new AttributeValueV1().withM(
        av.m().asScala.view.mapValues(sdk2ToSdk1AttributeValue).toMap.asJava
      )
    } else if (av.n() != null) {
      new AttributeValueV1().withN(av.n())
    } else if (
      av.ns() != null && !av.ns().isInstanceOf[DefaultSdkAutoConstructList[_]]
    ) {
      new AttributeValueV1().withNS(av.ns())
    } else if (av.s() != null) {
      if (av.s().isEmpty) {
        new AttributeValueV1().withNULL(true)
      } else {
        new AttributeValueV1().withS(av.s())
      }
    } else if (
      av.ss() != null && !av.ss().isInstanceOf[DefaultSdkAutoConstructList[_]]
    ) {
      new AttributeValueV1().withSS(av.ss())
    } else {
      new AttributeValueV1().withNULL(true)
    }
  }

  def sdk1ToSdk2AttributeValue(av: AttributeValueV1): AttributeValue = {
    def builder = AttributeValue.builder()
    if (av.getNULL) {
      builder.nul(true).build()
    } else if (av.getBOOL != null) {
      builder.bool(av.getBOOL).build()
    } else if (av.getB != null) {
      builder.b(SdkBytes.fromByteBuffer(av.getB)).build()
    } else if (av.getBS != null) {
      builder.bs(av.getBS.asScala.map(SdkBytes.fromByteBuffer).asJava).build()
    } else if (av.getL != null) {
      builder.l(av.getL.asScala.map(sdk1ToSdk2AttributeValue).asJava).build()
    } else if (av.getM != null) {
      builder.m(
        av.getM.asScala.view.mapValues(sdk1ToSdk2AttributeValue).toMap.asJava
      ).build()
    } else if (av.getN != null) {
      builder.n(av.getN).build()
    } else if (av.getNS != null) {
      builder.ns(av.getNS).build()
    } else if (av.getS != null) {
      builder.s(av.getS).build()
    } else if (av.getSS != null) {
      builder.ss(av.getSS).build()
    } else {
      builder.nul(true).build()
    }
  }

  implicit def dynamoFormatToDecoder[T](df: DynamoFormat[T]): Decoder[T] =
    new Decoder[T] {
      def read(av: AttributeValue): Either[DecoderFailure, T] =
        df.read(sdk2ToSdk1AttributeValue(av)).left.map { err =>
          DecoderFailure(DynamoReadError.describe(err), None)
        }
    }

  implicit def dynamoFormatToEncoder[T](df: DynamoFormat[T]): Encoder[T] =
    new Encoder[T] {
      def write(a: T): AttributeValue =
        sdk1ToSdk2AttributeValue(df.write(a).toAttributeValue)
    }
}
