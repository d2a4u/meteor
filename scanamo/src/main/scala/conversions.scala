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

  def sdk2ToSdk1AttributeValue(v2: AttributeValue): AttributeValueV1 = {
    val v1 = new AttributeValueV1()
    if (v2.nul() != null) {
      v1.withNULL(true)
    } else if (v2.bool() != null) {
      v1.withBOOL(v2.bool())
    } else if (v2.b() != null) {
      v1.withB(v2.b().asByteBuffer())
    } else if (
      v2.bs() != null && !v2.bs().isInstanceOf[DefaultSdkAutoConstructList[_]]
    ) {
      v1.withBS(
        v2.bs().asScala.map(_.asByteBuffer()).asJava
      )
    } else if (
      v2.l() != null && !v2.l().isInstanceOf[DefaultSdkAutoConstructList[_]]
    ) {
      v1.withL(
        v2.l().asScala.map(sdk2ToSdk1AttributeValue).asJava
      )
    } else if (
      v2.m() != null && !v2.m().isInstanceOf[DefaultSdkAutoConstructMap[_, _]]
    ) {
      v1.withM(
        v2.m().asScala.map(
          kv => kv._1 -> sdk2ToSdk1AttributeValue(kv._2)
        ).toMap.asJava
      )
    } else if (v2.n() != null) {
      v1.withN(v2.n())
    } else if (
      v2.ns() != null && !v2.ns().isInstanceOf[DefaultSdkAutoConstructList[_]]
    ) {
      v1.withNS(v2.ns())
    } else if (v2.s() != null) {
      v1.withS(v2.s())
    } else if (
      v2.ss() != null && !v2.ss().isInstanceOf[DefaultSdkAutoConstructList[_]]
    ) {
      v1.withSS(v2.ss())
    } else {
      v1.withNULL(true)
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
        av.getM.asScala.map(
          kv => kv._1 -> sdk1ToSdk2AttributeValue(kv._2)
        ).toMap.asJava
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
