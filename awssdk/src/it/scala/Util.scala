package meteor

import java.net.URI
import java.util.UUID
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, IO, Resource, Sync, Timer}
import cats.implicits._
import meteor.api.hi.{CompositeTable, SimpleTable}
import org.scalacheck.Arbitrary
import software.amazon.awssdk.auth.credentials.{
  AwsCredentials,
  AwsCredentialsProviderChain
}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.concurrent.duration._

object Util {
  def retryOf[F[_]: Timer: Sync, T](
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
            Timer[F].sleep(interval) >> r.set(i + 1) >> f
          case _ =>
            new Exception("Max retry reached").raiseError[F, T]
        }
      }
    }
  }.flatten

  def partitionKeyTable[F[_]: Concurrent: Timer]
    : Resource[F, (Client[F], PartitionKeyTable[Id])] = {
    val hashKey = KeyDef[Id]("id", DynamoDbType.S)
    for {
      client <- Client.resource[F](dummyCred, localDynamo, Region.EU_WEST_1)
      randomName <- Resource.liftF(
        Sync[F].delay(s"meteor-test-${UUID.randomUUID()}")
      )
      table = PartitionKeyTable[Id](randomName, hashKey)
      _ <- Resource.make(
        client.createPartitionKeyTable(
          randomName,
          hashKey,
          BillingMode.PAY_PER_REQUEST
        )
      )(_ => client.deleteTable(randomName))
    } yield (client, table)
  }

  def simpleTable[F[_]: Concurrent: Timer]: Resource[F, SimpleTable[F, Id]] = {
    val hashKey = KeyDef[Id]("id", DynamoDbType.S)
    for {
      jClient <- Resource.fromAutoCloseable[F, DynamoDbAsyncClient] {
        Sync[F].delay(DynamoDbAsyncClient.builder().credentialsProvider(
          dummyCred
        ).endpointOverride(localDynamo).build())
      }
      client = Client[F](jClient)
      randomName <- Resource.liftF(
        Sync[F].delay(s"meteor-test-${UUID.randomUUID()}")
      )
      _ <- Resource.make(
        client.createPartitionKeyTable(
          randomName,
          hashKey,
          BillingMode.PAY_PER_REQUEST
        )
      )(_ => client.deleteTable(randomName))
    } yield SimpleTable[F, Id](randomName, hashKey, jClient)
  }

  def compositeKeysTable[F[_]: Concurrent: Timer]
    : Resource[F, (Client[F], CompositeKeysTable[Id, Range])] = {
    val hashKey = KeyDef[Id]("id", DynamoDbType.S)
    val rangeKey = KeyDef[Range]("range", DynamoDbType.S)
    for {
      client <- Client.resource[F](dummyCred, localDynamo, Region.EU_WEST_1)
      randomName <- Resource.liftF(
        Sync[F].delay(s"meteor-test-${UUID.randomUUID()}")
      )
      table = CompositeKeysTable[Id, Range](randomName, hashKey, rangeKey)
      _ <- Resource.make(
        client.createCompositeKeysTable(
          randomName,
          hashKey,
          rangeKey,
          BillingMode.PAY_PER_REQUEST
        )
      )(_ => client.deleteTable(randomName))
    } yield (client, table)
  }

  def compositeTable[F[_]: Concurrent: Timer]
    : Resource[F, CompositeTable[F, Id, Range]] = {
    val hashKey = KeyDef[Id]("id", DynamoDbType.S)
    val rangeKey = KeyDef[Range]("range", DynamoDbType.S)
    for {
      jClient <- Resource.fromAutoCloseable[F, DynamoDbAsyncClient] {
        Sync[F].delay(DynamoDbAsyncClient.builder().build())
      }
      client <- Client.resource[F](dummyCred, localDynamo, Region.EU_WEST_1)
      randomName <- Resource.liftF(
        Sync[F].delay(s"meteor-test-${UUID.randomUUID()}")
      )
      table = CompositeTable[F, Id, Range](
        randomName,
        hashKey,
        rangeKey,
        jClient
      )
      _ <- Resource.make(
        client.createCompositeKeysTable(
          randomName,
          hashKey,
          rangeKey,
          BillingMode.PAY_PER_REQUEST
        )
      )(_ => client.deleteTable(randomName))
    } yield table
  }

  def compositeKeysWithSecondaryIndexTable[F[_]: Concurrent: Timer](
    indexName: String
  ): Resource[
    F,
    (
      Client[F],
      CompositeKeysTable[Id, Range],
      CompositeKeysSecondaryIndex[String, Int]
    )
  ] = {
    val hashKey1 = KeyDef[Id]("id", DynamoDbType.S)
    val rangeKey1 = KeyDef[Range]("range", DynamoDbType.S)
    val hashKey2 = KeyDef[String]("str", DynamoDbType.S)
    val rangeKey2 = KeyDef[Int]("int", DynamoDbType.N)
    for {
      client <- Client.resource[F](dummyCred, localDynamo, Region.EU_WEST_1)
      randomName <- Resource.liftF(
        Sync[F].delay(s"meteor-test-${UUID.randomUUID()}")
      )
      table = CompositeKeysTable[Id, Range](
        randomName,
        hashKey1,
        rangeKey1
      )
      index = CompositeKeysSecondaryIndex[String, Int](
        randomName,
        indexName,
        hashKey2,
        rangeKey2
      )
      _ <- Resource.make(
        client.createCompositeKeysTable[Id, Range](
          randomName,
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
              indexName
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
      )(_ => client.deleteTable(randomName))
    } yield (client, table, index)
  }

  def dummyCred: AwsCredentialsProviderChain =
    AwsCredentialsProviderChain.of(
      () =>
        new AwsCredentials {
          override def accessKeyId(): String = "DUMMY"
          override def secretAccessKey(): String = "DUMMY"
        }
    )

  def localDynamo: URI = URI.create("http://localhost:8000")

  def sample[T: Arbitrary]: T =
    implicitly[Arbitrary[T]].arbitrary.sample.get
}
