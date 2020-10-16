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
    table: Table,
    partitionKey: P,
    consistentRead: Boolean
  ): F[Option[T]]

  def get[T: Decoder, P: Encoder, S: Encoder](
    table: Table,
    partitionKey: P,
    sortKey: S,
    consistentRead: Boolean
  ): F[Option[T]]

  def retrieve[T: Decoder, P: Encoder, S: Encoder](
    table: Table,
    query: Query[P, S],
    consistentRead: Boolean,
    index: Option[Index] = None,
    limit: Int = Int.MaxValue
  ): fs2.Stream[F, T]

  def put[T: Encoder](
    table: Table,
    t: T
  ): F[Unit]

  def put[T: Encoder, U: Decoder](
    table: Table,
    t: T
  ): F[Option[U]]

  def put[T: Encoder](
    table: Table,
    t: T,
    condition: Expression
  ): F[Unit]

  def put[T: Encoder, U: Decoder](
    table: Table,
    t: T,
    condition: Expression
  ): F[Option[U]]

  def delete[P: Encoder, S: Encoder](
    table: Table,
    partitionKey: P,
    sortKey: S
  ): F[Unit]

  def delete[P: Encoder](
    table: Table,
    partitionKey: P
  ): F[Unit]

  def scan[T: Decoder](
    table: Table,
    filter: Expression,
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
