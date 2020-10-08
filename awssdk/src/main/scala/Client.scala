package meteor

import java.util.concurrent.Executor

import cats.effect.{Concurrent, Resource, Sync}
import meteor.codec.{Decoder, Encoder}
import software.amazon.awssdk.auth.credentials.{
  AwsCredentialsProviderChain,
  DefaultCredentialsProvider
}
import software.amazon.awssdk.core.client.config.{
  ClientAsyncConfiguration,
  SdkAdvancedAsyncClientOption
}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.TableDescription

import scala.jdk.CollectionConverters._

trait Client[F[_]] {

  def get[T: Decoder, P: Encoder](
    partitionKey: P,
    table: Table,
    consistentRead: Boolean
  ): F[Option[T]]

  def get[T: Decoder, P: Encoder, S: Encoder](
    partitionKey: P,
    sortKey: S,
    table: Table,
    consistentRead: Boolean
  ): F[Option[T]]

  def retrieve[T: Decoder, P: Encoder, S: Encoder](
    query: Query[P, S],
    table: Table,
    consistentRead: Boolean,
    index: Option[Index] = None,
    limit: Int = Int.MaxValue
  ): fs2.Stream[F, T]

  def put[T: Encoder](t: T, table: Table): F[Unit]

  def put[T: Encoder, U: Decoder](t: T, table: Table): F[Option[U]]

  def delete[P: Encoder, S: Encoder](
    partitionKey: P,
    sortKey: S,
    table: Table
  ): F[Unit]

  def scan[T: Decoder, P: Encoder, S: Encoder](
    query: Query[P, S],
    table: Table,
    consistentRead: Boolean,
    parallelism: Int
  ): fs2.Stream[F, Option[T]]

  def scan[T: Decoder](
    table: Table,
    consistentRead: Boolean,
    parallelism: Int
  ): fs2.Stream[F, Option[T]]

  def describe(table: Table): F[TableDescription]
}

object Client {
  def apply[F[_]: Concurrent](jClient: DynamoDbAsyncClient): Client[F] =
    new DefaultClient[F](jClient)

  def resource[F[_]: Concurrent]: Resource[F, Client[F]] = {
    Resource.fromAutoCloseable {
      Sync[F].delay(AwsCredentialsProviderChain.of(
        DefaultCredentialsProvider.create()
      ))
    }.flatMap { cred =>
      Resource.fromAutoCloseable {
        Sync[F].delay(
          DynamoDbAsyncClient.builder().credentialsProvider(cred).build()
        )
      }
    }.map(apply[F])
  }

  def resource[F[_]: Concurrent](exec: Executor): Resource[F, Client[F]] = {
    Resource.fromAutoCloseable {
      Sync[F].delay(AwsCredentialsProviderChain.of(
        DefaultCredentialsProvider.create()
      ))
    }.flatMap(cred => resource[F](exec, cred))
  }

  def resource[F[_]: Concurrent](
    exec: Executor,
    cred: AwsCredentialsProviderChain
  ): Resource[F, Client[F]] = {
    Resource.fromAutoCloseable {
      Sync[F].delay {
        val options: java.util.Map[SdkAdvancedAsyncClientOption[_], _] =
          Map(
            SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR -> exec
          ).asJava.asInstanceOf[java.util.Map[
            SdkAdvancedAsyncClientOption[_],
            _
          ]]
        val config =
          ClientAsyncConfiguration.builder().advancedOptions(options).build()
        DynamoDbAsyncClient.builder().asyncConfiguration(
          config
        ).credentialsProvider(cred).build()
      }
    }.map(apply[F])
  }
}
