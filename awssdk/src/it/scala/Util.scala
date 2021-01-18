package meteor

import java.net.URI
import java.util.UUID
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Resource, Sync, Timer}
import cats.implicits._
import software.amazon.awssdk.auth.credentials.{
  AwsCredentials,
  AwsCredentialsProviderChain
}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.model._

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

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

  def localTableWithSecondaryIndexResource[F[_]: Concurrent: Timer](
    hashKey: Key,
    rangeKey: Option[Key],
    attributeDefinition: Map[String, DynamoDbType],
    secondaryIndex: GlobalSecondaryIndex
  ): Resource[F, (Client[F], Table)] = {
    for {
      client <- Client.resource[F](dummyCred, localDynamo, Region.EU_WEST_1)
      randomName <- Resource.liftF(
        Sync[F].delay(s"meteor-test-${UUID.randomUUID()}")
      )
      table = Table(randomName, hashKey, rangeKey)
      _ <- Resource.make(
        client.createTable(
          table,
          attributeDefinition,
          Set(secondaryIndex),
          Set.empty,
          BillingMode.PAY_PER_REQUEST
        )
      )(_ => client.deleteTable(table.name))
    } yield (client, table)
  }

  def localTableResource[F[_]: Concurrent: Timer](
    hashKey: Key,
    rangeKey: Option[Key]
  ): Resource[F, (Client[F], Table)] = {
    for {
      client <- Client.resource[F](dummyCred, localDynamo, Region.EU_WEST_1)
      randomName <- Resource.liftF(
        Sync[F].delay(s"meteor-test-${UUID.randomUUID()}")
      )
      table = Table(randomName, hashKey, rangeKey)
      _ <- Resource.make(
        client.createTable(
          table,
          Map.empty,
          Set.empty,
          Set.empty,
          BillingMode.PAY_PER_REQUEST
        )
      )(_ => client.deleteTable(table.name))
    } yield (client, table)
  }

  def tableWithPartitionKey[F[_]: Concurrent: Timer] =
    localTableResource[F](
      Key(
        "id",
        DynamoDbType.S
      ),
      None
    )

  def tableWithKeys[F[_]: Concurrent: Timer] =
    localTableResource[F](
      Key(
        "id",
        DynamoDbType.S
      ),
      Some(
        Key(
          "range",
          DynamoDbType.S
        )
      )
    )

  def tableWithKeysAndSecondaryIndex[F[_]: Concurrent: Timer](
    indexName: String
  ) = {
    localTableWithSecondaryIndexResource[F](
      Key(
        "id",
        DynamoDbType.S
      ),
      Some(
        Key(
          "range",
          DynamoDbType.S
        )
      ),
      Map(
        "id" -> DynamoDbType.S,
        "range" -> DynamoDbType.S,
        "int" -> DynamoDbType.N,
        "str" -> DynamoDbType.S
      ),
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
    ).map {
      case (client, table) =>
        (
          client,
          table,
          SecondaryIndex(
            table.name,
            indexName,
            Key("str", DynamoDbType.S),
            Some(Key("int", DynamoDbType.N))
          )
        )
    }
  }

  def dummyCred =
    AwsCredentialsProviderChain.of(
      () =>
        new AwsCredentials {
          override def accessKeyId(): String = "DUMMY"
          override def secretAccessKey(): String = "DUMMY"
        }
    )

  def localDynamo = URI.create("http://localhost:8000")
}
