package meteor

import cats.effect.{IO, Resource, Sync}
import cats.effect.concurrent.Ref
import com.amazonaws.services.dynamodbv2.{
  AmazonDynamoDBAsync,
  AmazonDynamoDBAsyncClient
}
import meteor.eventstore.EventStore
import org.scanamo.error.{DynamoReadError, MissingProperty}
import org.scanamo.{DynamoFormat, DynamoValue}
import software.amazon.awssdk.auth.credentials.{
  AwsCredentialsProviderChain,
  DefaultCredentialsProvider
}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

class ScanOpsSpec extends ITSpec {

  behavior.of("scan operation")
  val tableName = Table("test_scan")
  implicit val dynamoFormatTestDataScan: DynamoFormat[TestDataScan] =
    new DynamoFormat[TestDataScan] {
      override def read(dv: DynamoValue)
        : Either[DynamoReadError, TestDataScan] = {
        val av = dv.toAttributeValue.getM
        for {
          id <- Option(av.get("id").getS).toRight(MissingProperty)
          range <- Option(av.get("range").getS).toRight(MissingProperty)
          data <- Option(av.get("data").getS).toRight(MissingProperty)
        } yield TestDataScan(Id(id), Range(range), data)
      }

      override def write(t: TestDataScan): DynamoValue = {
        val m = Map(
          "id" -> t.id.value,
          "range" -> t.range.value,
          "data" -> t.data
        )
        DynamoFormat[Map[String, String]].write(m)
      }
    }

  it should "return the whole table - eventstore" in {
    val ref = Ref.of[IO, Int](0)
    val es: Resource[IO, EventStore[IO]] =
      Resource.make[IO, AmazonDynamoDBAsync](
        Sync[IO].delay(AmazonDynamoDBAsyncClient.asyncBuilder().build())
      )(client => Sync[IO].delay(client.shutdown())).map { client =>
        new EventStore[IO](client, tableName.name)
      }

    def updated(ref: Ref[IO, Int]) =
      for {
        c <- fs2.Stream.resource(es)
        void <- c.scan[TestDataScan]().evalMap { _ =>
          ref.update(_ + 1)
        }
      } yield void

    val result =
      for {
        r <- ref
        _ <- updated(r).compile.drain
        i <- r.get
      } yield i
    result.unsafeRunSync() shouldEqual 1000
  }

  it should "return the whole table - eventstore2" in {
    val ref = Ref.of[IO, Int](0)
    val jClient = Resource.fromAutoCloseable {
      Sync[IO].delay(AwsCredentialsProviderChain.of(
        DefaultCredentialsProvider.create()
      ))
    }.flatMap { cred =>
      Resource.fromAutoCloseable {
        Sync[IO].delay(
          DynamoDbAsyncClient.builder().credentialsProvider(cred).build()
        )
      }
    }
    val es: Resource[IO, EventStore2[IO]] =
      jClient.map { client =>
        new EventStore2[IO](client, tableName.name)
      }

    def updated(ref: Ref[IO, Int]) =
      for {
        c <- fs2.Stream.resource(es)
        void <- c.scan[TestDataScan]().evalMap { _ =>
          ref.update(_ + 1)
        }
      } yield void

    val result =
      for {
        r <- ref
        _ <- updated(r).compile.drain
        i <- r.get
      } yield i
    result.unsafeRunSync() shouldEqual 1000
  }

  it should "return the whole table - meteor" in {
    val ref = Ref.of[IO, Int](0)
    val client = Client.resource[IO]
    def updated(ref: Ref[IO, Int]) =
      for {
        c <- fs2.Stream.resource(client)
        void <- c.scan[TestDataScan](
          tableName,
          consistentRead = false,
          32
        ).evalMap { _ =>
          ref.update(_ + 1)
        }
      } yield void

    val result =
      for {
        r <- ref
        _ <- updated(r).compile.drain
        i <- r.get
      } yield i
    result.unsafeRunSync() shouldEqual 1000
  }

}
