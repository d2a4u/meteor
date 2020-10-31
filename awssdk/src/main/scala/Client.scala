package meteor

import java.net.URI
import java.util.concurrent.Executor

import cats.effect.{Concurrent, Resource, Sync, Timer}
import fs2.Pipe
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
import software.amazon.awssdk.services.dynamodb.model.{
  BillingMode,
  KeyType,
  ReturnValue,
  ScalarAttributeType,
  TableDescription
}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

trait Client[F[_]] {

  def get[U: Decoder, P: Encoder](
    table: Table,
    partitionKey: P,
    consistentRead: Boolean
  ): F[Option[U]]

  def get[U: Decoder, P: Encoder, S: Encoder](
    table: Table,
    partitionKey: P,
    sortKey: S,
    consistentRead: Boolean
  ): F[Option[U]]

  def retrieve[U: Decoder, P: Encoder, S: Encoder](
    table: Table,
    query: Query[P, S],
    consistentRead: Boolean,
    limit: Int
  ): fs2.Stream[F, U]

  def retrieve[U: Decoder, P: Encoder, S: Encoder](
    table: Table,
    query: Query[P, S],
    consistentRead: Boolean,
    index: Index,
    limit: Int
  ): fs2.Stream[F, U]

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

  def scan[U: Decoder](
    table: Table,
    filter: Expression,
    consistentRead: Boolean,
    parallelism: Int
  ): fs2.Stream[F, U]

  def scan[U: Decoder](
    table: Table,
    consistentRead: Boolean,
    parallelism: Int
  ): fs2.Stream[F, U]

  def update[P: Encoder, U: Decoder](
    table: Table,
    partitionKey: P,
    update: Expression,
    returnValue: ReturnValue
  ): F[Option[U]]

  def update[P: Encoder, U: Decoder](
    table: Table,
    partitionKey: P,
    update: Expression,
    condition: Expression,
    returnValue: ReturnValue
  ): F[Option[U]]

  def update[P: Encoder, S: Encoder, U: Decoder](
    table: Table,
    partitionKey: P,
    sortKey: S,
    update: Expression,
    returnValue: ReturnValue
  ): F[Option[U]]

  def update[P: Encoder, S: Encoder, U: Decoder](
    table: Table,
    partitionKey: P,
    sortKey: S,
    update: Expression,
    condition: Expression,
    returnValue: ReturnValue
  ): F[Option[U]]

  def update[P: Encoder](
    table: Table,
    partitionKey: P,
    update: Expression
  ): F[Unit]

  def update[P: Encoder](
    table: Table,
    partitionKey: P,
    update: Expression,
    condition: Expression
  ): F[Unit]

  def update[P: Encoder, S: Encoder](
    table: Table,
    partitionKey: P,
    sortKey: S,
    update: Expression
  ): F[Unit]

  def update[P: Encoder, S: Encoder](
    table: Table,
    partitionKey: P,
    sortKey: S,
    update: Expression,
    condition: Expression
  ): F[Unit]

  def batchPut[T: Encoder](
    table: Table,
    maxBatchWait: FiniteDuration,
    parallelism: Int
  ): Pipe[F, T, Unit]

  def batchDelete[P: Encoder](
    table: Table,
    maxBatchWait: FiniteDuration,
    parallelism: Int
  ): Pipe[F, P, Unit]

  def batchDelete[P: Encoder, S: Encoder](
    table: Table,
    maxBatchWait: FiniteDuration,
    parallelism: Int
  ): Pipe[F, (P, S), Unit]

  def describe(table: Table): F[TableDescription]

  def createTable(
    table: Table,
    keys: Map[String, (KeyType, ScalarAttributeType)],
    billingMode: BillingMode
  ): F[Unit]

  def deleteTable(table: Table): F[Unit]
}

object Client {
  def apply[F[_]: Concurrent: Timer](jClient: DynamoDbAsyncClient): Client[F] =
    new DefaultClient[F](jClient)

  def resource[F[_]: Concurrent: Timer](
    cred: AwsCredentialsProviderChain,
    endpoint: URI
  ): Resource[F, Client[F]] =
    Resource.fromAutoCloseable {
      Sync[F].delay(
        DynamoDbAsyncClient.builder().credentialsProvider(
          cred
        ).endpointOverride(endpoint).build()
      )
    }.map(apply[F])

  def resource[F[_]: Concurrent: Timer]: Resource[F, Client[F]] = {
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

  def resource[F[_]: Concurrent: Timer](exec: Executor)
    : Resource[F, Client[F]] = {
    Resource.fromAutoCloseable {
      Sync[F].delay(AwsCredentialsProviderChain.of(
        DefaultCredentialsProvider.create()
      ))
    }.flatMap(cred => resource[F](exec, cred))
  }

  def resource[F[_]: Concurrent: Timer](
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
