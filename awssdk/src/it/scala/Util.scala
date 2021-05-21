package meteor

import java.net.URI
import java.util.UUID
import cats.effect.{Async, Ref, Resource, Sync, Temporal}
import cats.implicits._
import meteor.api.hi._
import org.scalacheck.Arbitrary
import software.amazon.awssdk.auth.credentials.{
  AwsCredentials,
  AwsCredentialsProviderChain
}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.concurrent.duration._

private[meteor] object Util {
  type SimpleTableWithGlobIndex[F[_], T] = SimpleTable[F, T]
  type CompositeTableWithGlobIndex[F[_], T, U] = CompositeTable[F, T, U]
  type CompositeKeysTableWithGlobIndex[T, U] = CompositeKeysTable[T, U]

  def retryOf[F[_]: Async, T](
    f: F[T],
    interval: FiniteDuration = 1.second,
    maxRetry: Int = 10
  )(cond: T => Boolean): F[T] = {
    def ref = Ref.of[F, Int](0)

    for {
      r <- ref
      t <- f
    } yield {
      if (cond(t)) {
        t.pure[F]
      } else {
        r.get.flatMap {
          case i if i < maxRetry =>
            Temporal[F].sleep(interval) >> r.set(i + 1) >> f
          case _ =>
            new Exception("Max retry reached").raiseError[F, T]
        }
      }
    }
  }.flatten

  def partitionKeyTable[F[_]: Async]
    : Resource[F, (Client[F], PartitionKeyTable[Id])] =
    internalSimpleResources[F].map {
      case (c, t, _, _, _) => (c, t)
    }

  def simpleTable[F[_]: Async]: Resource[F, SimpleTable[F, Id]] =
    internalSimpleResources[F].map {
      case (_, _, t, _, _) => t
    }

  def secondarySimpleIndex[F[_]: Async]
    : Resource[F, (SimpleTable[F, Id], SecondarySimpleIndex[F, Range])] =
    internalSimpleResources[F].map {
      case (_, _, _, t, i) => (t, i)
    }

  def compositeKeysTable[F[_]: Async]
    : Resource[F, (Client[F], CompositeKeysTable[Id, Range])] =
    internalCompositeResources[F].map {
      case (c, t, _, _, _, _, _) => (c, t)
    }

  def compositeTable[F[_]: Async]: Resource[F, CompositeTable[F, Id, Range]] =
    internalCompositeResources[F].map {
      case (_, _, _, _, t, _, _) => t
    }

  def secondaryCompositeIndex[F[_]: Async]: Resource[
    F,
    (CompositeTable[F, Id, Range], SecondaryCompositeIndex[F, String, Int])
  ] =
    internalCompositeResources[F].map {
      case (_, _, _, _, _, t, i) => (t, i)
    }

  def compositeKeysWithSecondaryIndexTable[F[_]: Async]: Resource[
    F,
    (
      Client[F],
      CompositeKeysTable[Id, Range],
      CompositeKeysSecondaryIndex[String, Int]
    )
  ] =
    internalCompositeResources[F].map {
      case (c, _, t, i, _, _, _) => (c, t, i)
    }

  private def genName[F[_]: Sync](prefix: String): Resource[F, String] = {
    Resource.eval[F, String](
      Sync[F].delay(s"$prefix-${UUID.randomUUID()}")
    )
  }

  private def jClientSrc[F[_]: Sync] = {
    Resource.fromAutoCloseable[F, DynamoDbAsyncClient] {
      Sync[F].delay(DynamoDbAsyncClient.builder().credentialsProvider(
        dummyCred
      ).endpointOverride(localDynamo).region(Region.EU_WEST_1).build())
    }
  }

  private def internalSimpleResources[F[_]: Async]: Resource[
    F,
    (
      Client[F],
      PartitionKeyTable[Id],
      SimpleTable[F, Id],
      SimpleTableWithGlobIndex[F, Id],
      SecondarySimpleIndex[F, Range]
    )
  ] = {
    val hashKey1 = KeyDef[Id]("id", DynamoDbType.S)
    val glob2ndHashKey = KeyDef[Range]("range", DynamoDbType.S)
    for {
      jClient <- jClientSrc[F]
      client = Client[F](jClient)
      simpleRandomName <- genName("simple-table")
      simpleRandomNameWithGlobIndex <- genName("simple-table-with-glob-index")
      simpleRandomIndexName <- genName("global-secondary-simple-index")
      pkt = PartitionKeyTable[Id](
        simpleRandomName,
        hashKey1
      )
      st = SimpleTable[F, Id](
        simpleRandomName,
        hashKey1,
        jClient
      )
      stgi = SimpleTable[F, Id](
        simpleRandomNameWithGlobIndex,
        hashKey1,
        jClient
      )
      ssi = SecondarySimpleIndex[F, Range](
        simpleRandomNameWithGlobIndex,
        simpleRandomIndexName,
        glob2ndHashKey,
        jClient
      )
      _ <- Resource.make(
        client.createPartitionKeyTable(
          simpleRandomName,
          hashKey1,
          BillingMode.PAY_PER_REQUEST
        )
      )(_ => client.deleteTable(simpleRandomName))
      _ <- Resource.make(
        client.createPartitionKeyTable(
          simpleRandomNameWithGlobIndex,
          hashKey1,
          BillingMode.PAY_PER_REQUEST,
          Map(hashKey1.value, glob2ndHashKey.value),
          Set(
            GlobalSecondaryIndex
              .builder()
              .indexName(simpleRandomIndexName)
              .keySchema(
                KeySchemaElement.builder().attributeName("range").keyType(
                  KeyType.HASH
                ).build()
              ).projection(Projection.builder().projectionType(
                ProjectionType.ALL
              ).build())
              .build()
          )
        )
      )(_ => client.deleteTable(simpleRandomNameWithGlobIndex))
    } yield (client, pkt, st, stgi, ssi)
  }

  private def internalCompositeResources[F[_]: Async]: Resource[
    F,
    (
      Client[F],
      CompositeKeysTable[Id, Range],
      CompositeKeysTableWithGlobIndex[Id, Range],
      CompositeKeysSecondaryIndex[String, Int],
      CompositeTable[F, Id, Range],
      CompositeTableWithGlobIndex[F, Id, Range],
      SecondaryCompositeIndex[F, String, Int]
    )
  ] = {
    val hashKey1 = KeyDef[Id]("id", DynamoDbType.S)
    val rangeKey1 = KeyDef[Range]("range", DynamoDbType.S)
    val hashKey2 = KeyDef[String]("str", DynamoDbType.S)
    val rangeKey2 = KeyDef[Int]("int", DynamoDbType.N)

    for {
      jClient <- jClientSrc[F]
      client = Client[F](jClient)
      compositeRandomName <- genName("composite-table")
      compositeRandomNameWithGlobIndex <- genName(
        "composite-table-with-glob-index"
      )
      compositeRandomIndexName <- genName("global-secondary-composite-index")
      ckt = CompositeKeysTable[Id, Range](
        compositeRandomName,
        hashKey1,
        rangeKey1
      )
      cktsi = CompositeKeysTable[Id, Range](
        compositeRandomNameWithGlobIndex,
        hashKey1,
        rangeKey1
      )
      cksi = CompositeKeysSecondaryIndex[String, Int](
        compositeRandomNameWithGlobIndex,
        compositeRandomIndexName,
        hashKey2,
        rangeKey2
      )
      ct = CompositeTable[F, Id, Range](
        compositeRandomName,
        hashKey1,
        rangeKey1,
        jClient
      )
      ctwg = CompositeTable[F, Id, Range](
        compositeRandomNameWithGlobIndex,
        hashKey1,
        rangeKey1,
        jClient
      )
      sci = SecondaryCompositeIndex[F, String, Int](
        compositeRandomNameWithGlobIndex,
        compositeRandomIndexName,
        hashKey2,
        rangeKey2,
        jClient
      )
      _ <- Resource.make(
        client.createCompositeKeysTable[Id, Range](
          compositeRandomName,
          hashKey1,
          rangeKey1,
          BillingMode.PAY_PER_REQUEST
        )
      )(_ => client.deleteTable(compositeRandomName))
      _ <- Resource.make(
        client.createCompositeKeysTable[Id, Range](
          compositeRandomNameWithGlobIndex,
          hashKey1,
          rangeKey1,
          BillingMode.PAY_PER_REQUEST,
          Map(
            "id" -> DynamoDbType.S,
            "range" -> DynamoDbType.S,
            "int" -> DynamoDbType.N,
            "str" -> DynamoDbType.S
          ),
          Set(
            GlobalSecondaryIndex.builder().indexName(
              compositeRandomIndexName
            ).keySchema(
              KeySchemaElement.builder().attributeName("str").keyType(
                KeyType.HASH
              ).build(),
              KeySchemaElement.builder().attributeName("int").keyType(
                KeyType.RANGE
              ).build()
            ).projection(Projection.builder().projectionType(
              ProjectionType.ALL
            ).build()).build()
          ),
          Set.empty
        )
      )(_ => client.deleteTable(compositeRandomNameWithGlobIndex))
    } yield (client, ckt, cktsi, cksi, ct, ctwg, sci)
  }

  def dummyCred: AwsCredentialsProviderChain =
    AwsCredentialsProviderChain.of(() =>
      new AwsCredentials {
        override def accessKeyId(): String = "DUMMY"
        override def secretAccessKey(): String = "DUMMY"
      })

  def localDynamo: URI = URI.create("http://localhost:8000")

  def sample[T: Arbitrary]: T =
    implicitly[Arbitrary[T]].arbitrary.sample.get
}
