package meteor

import java.net.URI
import java.util.concurrent.Executor
import cats.effect.{Concurrent, Resource, Sync, Timer}
import fs2.{Pipe, RaiseThrowable, Stream}
import meteor.api.BatchGet
import meteor.codec.{Decoder, Encoder}
import software.amazon.awssdk.auth.credentials.{
  AwsCredentialsProviderChain,
  DefaultCredentialsProvider
}
import software.amazon.awssdk.core.client.config.{
  ClientAsyncConfiguration,
  SdkAdvancedAsyncClientOption
}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  BillingMode,
  KeyType,
  ReturnValue,
  ScalarAttributeType,
  TableDescription
}

import scala.collection.immutable.Iterable
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

trait Client[F[_]] {

  /**
    * Get a single value from a table by partition key P.
    */
  def get[U: Decoder, P: Encoder](
    tableName: String,
    partitionKey: P,
    consistentRead: Boolean
  ): F[Option[U]]

  /**
    * Get a single value from a table by partition key P and sort key S.
    */
  def get[U: Decoder, P: Encoder, S: Encoder](
    tableName: String,
    partitionKey: P,
    sortKey: S,
    consistentRead: Boolean
  ): F[Option[U]]

  /**
    * Retrieve values from a table using a query.
    */
  def retrieve[U: Decoder, P: Encoder, S: Encoder](
    tableName: String,
    query: Query[P, S],
    consistentRead: Boolean,
    limit: Int
  ): fs2.Stream[F, U]

  /**
    * Retrieve values from a table using a query on a secondary index.
    */
  def retrieve[U: Decoder, P: Encoder, S: Encoder](
    tableName: String,
    query: Query[P, S],
    consistentRead: Boolean,
    index: Index,
    limit: Int
  ): fs2.Stream[F, U]

  /**
    * Retrieve values from a table by partition key P.
    */
  def retrieve[
    U: Decoder,
    P: Encoder
  ](
    tableName: String,
    partitionKey: P,
    consistentRead: Boolean,
    limit: Int
  ): fs2.Stream[F, U]

  /**
    * Retrieve values from a table by partition key P on a secondary index.
    */
  def retrieve[
    U: Decoder,
    P: Encoder
  ](
    tableName: String,
    partitionKey: P,
    consistentRead: Boolean,
    index: Index,
    limit: Int
  ): fs2.Stream[F, U]

  /**
    * Put an item into a table, return Unit (ReturnValue.NONE).
    */
  def put[T: Encoder](
    tableName: String,
    t: T
  ): F[Unit]

  /**
    * Put an item into a table, return ReturnValue.ALL_OLD.
    */
  def put[T: Encoder, U: Decoder](
    tableName: String,
    t: T
  ): F[Option[U]]

  /**
    * Put an item into a table with a condition expression, return Unit (ReturnValue.NONE).
    */
  def put[T: Encoder](
    tableName: String,
    t: T,
    condition: Expression
  ): F[Unit]

  /**
    * Put an item into a table with a condition expression, return ReturnValue.ALL_OLD.
    */
  def put[T: Encoder, U: Decoder](
    tableName: String,
    t: T,
    condition: Expression
  ): F[Option[U]]

  /**
    * Delete an item from a table by partition key P and sort key S.
    */
  def delete[P: Encoder, S: Encoder](
    tableName: String,
    partitionKey: P,
    sortKey: S
  ): F[Unit]

  /**
    * Delete an item from a table by partition key P.
    */
  def delete[P: Encoder](
    tableName: String,
    partitionKey: P
  ): F[Unit]

  /**
    * Scan the whole table with a filter expression.
    */
  def scan[U: Decoder](
    tableName: String,
    filter: Expression,
    consistentRead: Boolean,
    parallelism: Int
  ): fs2.Stream[F, U]

  /**
    * Scan the whole table.
    */
  def scan[U: Decoder](
    tableName: String,
    consistentRead: Boolean,
    parallelism: Int
  ): fs2.Stream[F, U]

  /**
    * Update an item by partition key P given an update expression.
    * A Codec of U is required to deserialize return value.
    */
  def update[P: Encoder, U: Decoder](
    tableName: String,
    partitionKey: P,
    update: Expression,
    returnValue: ReturnValue
  ): F[Option[U]]

  /**
    * Update an item by partition key P given an update expression
    * when it fulfills a condition expression.
    * A Codec of U is required to deserialize return value.
    */
  def update[P: Encoder, U: Decoder](
    tableName: String,
    partitionKey: P,
    update: Expression,
    condition: Expression,
    returnValue: ReturnValue
  ): F[Option[U]]

  /**
    * Update an item by partition key P and a sort key S, given an update expression.
    * A Codec of U is required to deserialize return value.
    */
  def update[P: Encoder, S: Encoder, U: Decoder](
    tableName: String,
    partitionKey: P,
    sortKey: S,
    update: Expression,
    returnValue: ReturnValue
  ): F[Option[U]]

  /**
    * Update an item by partition key P and a sort key S, given an update expression
    * when it fulfills a condition expression.
    * A Codec of U is required to deserialize return value.
    */
  def update[P: Encoder, S: Encoder, U: Decoder](
    tableName: String,
    partitionKey: P,
    sortKey: S,
    update: Expression,
    condition: Expression,
    returnValue: ReturnValue
  ): F[Option[U]]

  /**
    * Update an item by partition key P given an update expression.
    * Return Unit (ReturnValue.NONE).
    */
  def update[P: Encoder](
    tableName: String,
    partitionKey: P,
    update: Expression
  ): F[Unit]

  /**
    * Update an item by partition key P given an update expression
    * when it fulfills a condition expression.
    * Return Unit (ReturnValue.NONE).
    */
  def update[P: Encoder](
    tableName: String,
    partitionKey: P,
    update: Expression,
    condition: Expression
  ): F[Unit]

  /**
    * Update an item by partition key P and a sort key S, given an update expression.
    * Return Unit (ReturnValue.NONE).
    */
  def update[P: Encoder, S: Encoder](
    tableName: String,
    partitionKey: P,
    sortKey: S,
    update: Expression
  ): F[Unit]

  /**
    * Update an item by partition key P and a sort key S, given an update expression
    * when it fulfills a condition expression.
    * Return Unit (ReturnValue.NONE).
    */
  def update[P: Encoder, S: Encoder](
    tableName: String,
    partitionKey: P,
    sortKey: S,
    update: Expression,
    condition: Expression
  ): F[Unit]

  /**
    * Batch get items from multiple tables.
    */
  def batchGet(
    requests: Map[String, BatchGet]
  ): F[Map[String, Iterable[AttributeValue]]]

  /**
    * Batch get items from a table by key(s) T.
    * A Codec of U is required to deserialize return value based on projection expression.
    */
  def batchGet[T: Encoder, U: Decoder](
    tableName: String,
    consistentRead: Boolean,
    projection: Expression,
    keys: Iterable[T]
  ): F[Iterable[U]]

  /**
    * Batch get items from a table by key(s) T.
    * A Codec of U is required to deserialize return value based on projection expression.
    */
  def batchGet[T: Encoder, U: Decoder](
    tableName: String,
    consistentRead: Boolean,
    projection: Expression,
    maxBatchWait: FiniteDuration,
    parallelism: Int
  ): Pipe[F, T, U]

  /**
    * Batch get items from a table by key(s) T.
    * A Codec of U is required to deserialize return value based on projection expression.
    */
  def batchGet[T: Encoder, U: Decoder](
    tableName: String,
    consistentRead: Boolean,
    keys: Iterable[T]
  ): F[Iterable[U]]

  /**
    * Batch put items into a table by a Stream of T.
    *
    * Deduplication logic within a batch is:
    *
    * within a batch, only send the the last item and discard all previous duplicated items with the
    * same key. Order from upstream is preserved to ensure that only putting the last version of T
    * within a batch.
    */
  def batchPut[T: Encoder](
    table: Table,
    maxBatchWait: FiniteDuration
  ): Pipe[F, T, Unit]

  /**
    * Batch put items into a table by a Seq of T.
    *
    * Deduplication logic within a batch is:
    *
    * within a batch, only send the the last item and discard all previous duplicated items with the
    * same key. Order from upstream is preserved to ensure that only putting the last version of T
    * within a batch.
    */
  def batchPut[T: Encoder](
    table: Table,
    items: Iterable[T]
  ): F[Unit]

  /**
    * Batch put unique items into a table.
    */
  def batchPutUnordered[T: Encoder](
    tableName: String,
    items: Set[T],
    maxBatchWait: FiniteDuration,
    parallelism: Int
  ): F[Unit]

  /**
    * Batch delete items from a table by a Stream of partition key P.
    *
    * Deduplication logic within a batch is:
    *
    * within a batch, only send deletion request for one P key and discard all duplicates.
    */
  def batchDelete[P: Encoder](
    tableName: String,
    maxBatchWait: FiniteDuration,
    parallelism: Int
  ): Pipe[F, P, Unit]

  /**
    * Batch delete items from a table by a Stream of partition key P and sort key S.
    *
    * Deduplication logic within a batch is:
    *
    * within a batch, only send deletion request for one (P, S) key pair and discard all duplicates.
    */
  def batchDelete[P: Encoder, S: Encoder](
    tableName: String,
    maxBatchWait: FiniteDuration,
    parallelism: Int
  ): Pipe[F, (P, S), Unit]

  /**
    * Batch write to a table by a Stream of either D (delete) or P (put).
    * A function identifiable: P => D is required to deduplicate requests within a batch.
    *
    * Deduplication logic within a batch is:
    *
    * Group all P and D items by key, preserve ordering from upstream.
    * If the last item for that key is a delete, then only perform deletion,
    * discard all other actions on that key.
    * If the last item for that key is a put, then only perform put,
    * discard all other actions on that key.
    */
  def batchWrite[D: Encoder, P: Encoder](
    table: Table,
    maxBatchWait: FiniteDuration
  ): Pipe[F, Either[D, P], Unit]

  /**
    * Describe a table
    */
  def describe(tableName: String): F[TableDescription]

  /**
    * Create a table
    */
  def createTable(
    table: Table,
    billingMode: BillingMode
  ): F[Unit]

  /**
    * Delete a table
    */
  def deleteTable(tableName: String): F[Unit]
}

object Client {
  def apply[F[_]: Concurrent: Timer](jClient: DynamoDbAsyncClient): Client[F] =
    new DefaultClient[F](jClient)

  def resource[F[_]: Concurrent: Timer](
    cred: AwsCredentialsProviderChain,
    endpoint: URI,
    region: Region
  ): Resource[F, Client[F]] =
    Resource.fromAutoCloseable {
      Sync[F].delay(
        DynamoDbAsyncClient.builder().credentialsProvider(
          cred
        ).endpointOverride(endpoint).region(region).build()
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
