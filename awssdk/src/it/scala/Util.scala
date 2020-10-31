package meteor

import java.net.URI
import java.util.UUID

import cats._
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Resource, Sync, Timer}
import cats.implicits._
import software.amazon.awssdk.auth.credentials.{
  AwsCredentials,
  AwsCredentialsProviderChain
}
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

  def resource[F[_]: Monad, G[_]: Traverse, T, U](
    gt: G[T],
    pre: T => F[U],
    post: U => F[Unit]
  ): Resource[F, G[U]] =
    Resource.make {
      gt.traverse(pre)
    } {
      _.traverse(
        post
      ).map(_.combineAll)
    }

  def localTableResource[F[_]: Concurrent: Timer](
    keys: Map[String, (KeyType, ScalarAttributeType)]
  ): Resource[F, (Client[F], Table)] = {
    for {
      client <- Client.resource[F](dummyCred, localDynamo)
      randomName <- Resource.liftF(
        Sync[F].delay(s"meteor-test-${UUID.randomUUID()}")
      )
      table = Table(randomName)
      _ <- Resource.make(
        client.createTable(
          table,
          keys,
          BillingMode.PAY_PER_REQUEST
        )
      )(_ => client.deleteTable(table))
    } yield (client, table)
  }

  def hasPrimaryKeys =
    Map(
      "id" -> (KeyType.HASH, ScalarAttributeType.S),
      "range" -> (KeyType.RANGE, ScalarAttributeType.S)
    )

  def hasPartitionKeyOnly =
    Map(
      "id" -> (KeyType.HASH, ScalarAttributeType.S)
    )

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
