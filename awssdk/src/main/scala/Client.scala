package meteor

import cats.effect.{Async, Resource, Sync}
import fs2.Pipe
import meteor.api.BatchGet
import meteor.codec.{Decoder, Encoder}
import software.amazon.awssdk.auth.credentials.{
  AwsCredentialsProvider,
  DefaultCredentialsProvider
}
import software.amazon.awssdk.core.client.config.{
  ClientAsyncConfiguration,
  SdkAdvancedAsyncClientOption
}
import software.amazon.awssdk.core.retry.backoff.{
  BackoffStrategy,
  FullJitterBackoffStrategy
}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  BillingMode,
  GlobalSecondaryIndex,
  LocalSecondaryIndex,
  ReturnValue,
  TableDescription
}

import java.net.URI
import java.time.Duration
import java.util.concurrent.Executor
import scala.collection.immutable.Iterable
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

trait Client[F[_]] {

  /**
    * Get a single value from a table by partition key P.
    */
  def get[P: Encoder, U: Decoder](
    table: PartitionKeyTable[P],
    partitionKey: P,
    consistentRead: Boolean
  ): F[Option[U]]

  /**
    * Get a single value from a table by partition key P and sort key S.
    */
  def get[P: Encoder, S: Encoder, U: Decoder](
    table: CompositeKeysTable[P, S],
    partitionKey: P,
    sortKey: S,
    consistentRead: Boolean
  ): F[Option[U]]

  /**
    * Retrieve values from a table using a query.
    */
  def retrieve[P: Encoder, S: Encoder, U: Decoder](
    index: CompositeKeysIndex[P, S],
    query: Query[P, S],
    consistentRead: Boolean,
    limit: Int
  ): fs2.Stream[F, U]

  /**
    * Retrieve values from a table by partition key P.
    */
  def retrieve[
    P: Encoder,
    U: Decoder
  ](
    index: CompositeKeysIndex[P, _],
    partitionKey: P,
    consistentRead: Boolean,
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
    table: CompositeKeysTable[P, S],
    partitionKey: P,
    sortKey: S
  ): F[Unit]

  /**
    * Delete an item from a table by partition key P.
    */
  def delete[P: Encoder](
    table: PartitionKeyTable[P],
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
    table: PartitionKeyTable[P],
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
    table: PartitionKeyTable[P],
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
    table: CompositeKeysTable[P, S],
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
    table: CompositeKeysTable[P, S],
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
    table: PartitionKeyTable[P],
    partitionKey: P,
    update: Expression
  ): F[Unit]

  /**
    * Update an item by partition key P given an update expression
    * when it fulfills a condition expression.
    * Return Unit (ReturnValue.NONE).
    */
  def update[P: Encoder](
    table: PartitionKeyTable[P],
    partitionKey: P,
    update: Expression,
    condition: Expression
  ): F[Unit]

  /**
    * Update an item by partition key P and a sort key S, given an update expression.
    * Return Unit (ReturnValue.NONE).
    */
  def update[P: Encoder, S: Encoder](
    table: CompositeKeysTable[P, S],
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
    table: CompositeKeysTable[P, S],
    partitionKey: P,
    sortKey: S,
    update: Expression,
    condition: Expression
  ): F[Unit]

  /**
    * Batch get items from multiple tables.
    */
  def batchGet(
    requests: Map[String, BatchGet],
    backoffStrategy: BackoffStrategy
  ): F[Map[String, Iterable[AttributeValue]]]

  /**
    * Batch get items from a table by partition key P.
    * A Codec of U is required to deserialize return value based on projection expression.
    */
  def batchGet[P: Encoder, U: Decoder](
    table: PartitionKeyTable[P],
    consistentRead: Boolean,
    projection: Expression,
    keys: Iterable[P],
    backoffStrategy: BackoffStrategy
  ): F[Iterable[U]]

  /**
    * Batch get items from a table primary keys P and S.
    * A Codec of U is required to deserialize return value based on projection expression.
    */
  def batchGet[P: Encoder, S: Encoder, U: Decoder](
    table: CompositeKeysTable[P, S],
    consistentRead: Boolean,
    projection: Expression,
    keys: Iterable[(P, S)],
    backoffStrategy: BackoffStrategy
  ): F[Iterable[U]]

  /**
    * Batch get items from a table by partition keys P.
    * A Codec of U is required to deserialize return value based on projection expression.
    */
  def batchGet[P: Encoder, U: Decoder](
    table: PartitionKeyTable[P],
    consistentRead: Boolean,
    projection: Expression,
    maxBatchWait: FiniteDuration,
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  ): Pipe[F, P, U]

  /**
    * Batch get items from a table by primary keys P and S.
    * A Codec of U is required to deserialize return value based on projection expression.
    */
  def batchGet[P: Encoder, S: Encoder, U: Decoder](
    table: CompositeKeysTable[P, S],
    consistentRead: Boolean,
    projection: Expression,
    maxBatchWait: FiniteDuration,
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  ): Pipe[F, (P, S), U]

  /**
    * Batch get items from a table by partition keys P.
    * A Codec of U is required to deserialize return value based on projection expression.
    */
  def batchGet[P: Encoder, U: Decoder](
    table: PartitionKeyTable[P],
    consistentRead: Boolean,
    keys: Iterable[P],
    backoffStrategy: BackoffStrategy
  ): F[Iterable[U]]

  /**
    * Batch get items from a table by primary keys P and S.
    * A Codec of U is required to deserialize return value based on projection expression.
    */
  def batchGet[P: Encoder, S: Encoder, U: Decoder](
    table: CompositeKeysTable[P, S],
    consistentRead: Boolean,
    keys: Iterable[(P, S)],
    backoffStrategy: BackoffStrategy
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
    table: Index,
    maxBatchWait: FiniteDuration,
    backoffStrategy: BackoffStrategy
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
    table: Index,
    items: Iterable[T],
    backoffStrategy: BackoffStrategy
  ): F[Unit]

  /**
    * Batch put unique items into a table.
    */
  def batchPutUnordered[T: Encoder](
    table: Index,
    items: Set[T],
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  ): F[Unit]

  /**
    * Batch delete items from a table by a Stream of partition key P.
    *
    * Deduplication logic within a batch is:
    *
    * within a batch, only send deletion request for one P key and discard all duplicates.
    */
  def batchDelete[P: Encoder](
    table: PartitionKeyTable[P],
    maxBatchWait: FiniteDuration,
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  ): Pipe[F, P, Unit]

  /**
    * Batch delete items from a table by a Stream of partition key P and sort key S.
    *
    * Deduplication logic within a batch is:
    *
    * within a batch, only send deletion request for one (P, S) key pair and discard all duplicates.
    */
  def batchDelete[P: Encoder, S: Encoder](
    table: CompositeKeysTable[P, S],
    maxBatchWait: FiniteDuration,
    parallelism: Int,
    backoffStrategy: BackoffStrategy
  ): Pipe[F, (P, S), Unit]

  /**
    * Batch write to a table by a Stream of either DP (delete) or P (put).
    *
    * DP: partition key's type
    * P: item being put's type
    *
    * Deduplication logic within a batch is:
    *
    * Group all DP and P items by key, preserve ordering from upstream.
    * If the last item for that key is a delete, then only perform deletion,
    * discard all other actions on that key.
    * If the last item for that key is a put, then only perform put,
    * discard all other actions on that key.
    */
  def batchWrite[DP: Encoder, P: Encoder](
    table: PartitionKeyTable[DP],
    maxBatchWait: FiniteDuration,
    backoffStrategy: BackoffStrategy
  ): Pipe[F, Either[DP, P], Unit]

  /**
    * Batch write to a table by a Stream of either (DP, DS) (delete) or P (put).
    *
    * DP: partition key's type
    * DS: sort key's type
    * P: item being put's type
    *
    * Deduplication logic within a batch is:
    *
    * Group all (DP, DS) and P items by key, preserve ordering from upstream.
    * If the last item for that key is a delete, then only perform deletion,
    * discard all other actions on that key.
    * If the last item for that key is a put, then only perform put,
    * discard all other actions on that key.
    */
  def batchWrite[DP: Encoder, DS: Encoder, P: Encoder](
    table: CompositeKeysTable[DP, DS],
    maxBatchWait: FiniteDuration,
    backoffStrategy: BackoffStrategy
  ): Pipe[F, Either[(DP, DS), P], Unit]

  /**
    * Describe a table
    */
  def describe(tableName: String): F[TableDescription]

  /**
    * Create a table
    */
  def createCompositeKeysTable[P, S](
    tableName: String,
    partitionKeyDef: KeyDef[P],
    sortKeyDef: KeyDef[S],
    billingMode: BillingMode,
    attributeDefinition: Map[String, DynamoDbType] = Map.empty,
    globalSecondaryIndexes: Set[GlobalSecondaryIndex] = Set.empty,
    localSecondaryIndexes: Set[LocalSecondaryIndex] = Set.empty
  ): F[Unit]

  def createPartitionKeyTable[P](
    tableName: String,
    partitionKeyDef: KeyDef[P],
    billingMode: BillingMode,
    attributeDefinition: Map[String, DynamoDbType] = Map.empty,
    globalSecondaryIndexes: Set[GlobalSecondaryIndex] = Set.empty,
    localSecondaryIndexes: Set[LocalSecondaryIndex] = Set.empty
  ): F[Unit]

  /**
    * Delete a table
    */
  def deleteTable(tableName: String): F[Unit]
}

object Client {
  def apply[F[_]: Async](jClient: DynamoDbAsyncClient): Client[F] =
    new DefaultClient[F](jClient)

  def resource[F[_]: Async](
    cred: AwsCredentialsProvider,
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

  def resource[F[_]: Async]: Resource[F, Client[F]] = {
    Resource.fromAutoCloseable {
      Sync[F].delay(DefaultCredentialsProvider.create())
    }.flatMap { cred =>
      Resource.fromAutoCloseable {
        Sync[F].delay(
          DynamoDbAsyncClient.builder().credentialsProvider(cred).build()
        )
      }
    }.map(apply[F])
  }

  def resource[F[_]: Async](exec: Executor): Resource[F, Client[F]] = {
    Resource.fromAutoCloseable {
      Sync[F].delay(DefaultCredentialsProvider.create())
    }.flatMap(cred => resource[F](exec, cred))
  }

  def resource[F[_]: Async](
    exec: Executor,
    cred: AwsCredentialsProvider
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

  object BackoffStrategy {
    // same as default from DynamoDbRetryPolicy
    val default: BackoffStrategy = FullJitterBackoffStrategy.builder.baseDelay(
      Duration.ofMillis(25)
    ).maxBackoffTime(Duration.ofMillis(20000)).build
  }
}
