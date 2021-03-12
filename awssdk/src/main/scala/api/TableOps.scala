package meteor
package api

import cats.effect.{Async, Temporal}
import cats.implicits._
import meteor.errors.UnexpectedTableStatus
import meteor.implicits._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.concurrent.duration.{DurationInt, FiniteDuration}

trait TableOps {
  def describeOp[F[_]: Async](tableName: String)(
    jClient: DynamoDbAsyncClient
  ): F[TableDescription] = {
    val req = DescribeTableRequest.builder().tableName(tableName).build()
    liftFuture(jClient.describeTable(req)).map { resp =>
      resp.table()
    }
  }

  def deleteTableOp[F[_]: Async](
    tableName: String
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    val deleteTableRequest =
      DeleteTableRequest.builder().tableName(tableName).build()
    liftFuture(jClient.deleteTable(deleteTableRequest)).void
  }

  def createCompositeKeysTableOp[
    F[_]: Async,
    P,
    S
  ](
    tableName: String,
    partitionKeyDef: KeyDef[P],
    sortKeyDef: KeyDef[S],
    attributeDefinition: Map[String, DynamoDbType],
    globalSecondaryIndexes: Set[GlobalSecondaryIndex],
    localSecondaryIndexes: Set[LocalSecondaryIndex],
    billingMode: BillingMode,
    waitTillReady: Boolean
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    val keySchema =
      List(
        dynamoKey(partitionKeyDef.attributeName, KeyType.HASH),
        dynamoKey(sortKeyDef.attributeName, KeyType.RANGE)
      )
    val attrDef = (attributeDefinition ++ Map(
      partitionKeyDef.attributeName -> partitionKeyDef.attributeType,
      sortKeyDef.attributeName -> sortKeyDef.attributeType
    )).map(kv => dynamoAttribute(kv._1, kv._2)).toList

    sendCreateTableRequest(
      tableName,
      keySchema,
      attrDef,
      globalSecondaryIndexes,
      localSecondaryIndexes,
      billingMode,
      waitTillReady
    )(jClient)
  }

  def createPartitionKeyTableOp[
    F[_]: Async,
    P
  ](
    tableName: String,
    partitionKeyDef: KeyDef[P],
    attributeDefinition: Map[String, DynamoDbType],
    globalSecondaryIndexes: Set[GlobalSecondaryIndex],
    localSecondaryIndexes: Set[LocalSecondaryIndex],
    billingMode: BillingMode,
    waitTillReady: Boolean
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    val keySchema =
      List(
        dynamoKey(partitionKeyDef.attributeName, KeyType.HASH)
      )
    val attrDef = (attributeDefinition ++ Map(
      partitionKeyDef.attributeName -> partitionKeyDef.attributeType
    )).map(kv => dynamoAttribute(kv._1, kv._2)).toList

    sendCreateTableRequest(
      tableName,
      keySchema,
      attrDef,
      globalSecondaryIndexes,
      localSecondaryIndexes,
      billingMode,
      waitTillReady
    )(jClient)
  }

  private def sendCreateTableRequest[
    F[_]: Async
  ](
    tableName: String,
    keySchema: List[KeySchemaElement],
    attributeDefinitions: List[AttributeDefinition],
    globalSecondaryIndexes: Set[GlobalSecondaryIndex],
    localSecondaryIndexes: Set[LocalSecondaryIndex],
    billingMode: BillingMode,
    waitTillReady: Boolean
  )(jClient: DynamoDbAsyncClient) = {
    val builder0 =
      CreateTableRequest
        .builder()
        .tableName(tableName)
        .billingMode(billingMode)

    val builder1 = builder0
      .keySchema(keySchema: _*)
      .attributeDefinitions(attributeDefinitions: _*)
    val builder2 = {
      if (globalSecondaryIndexes.isEmpty) builder1
      else builder1.globalSecondaryIndexes(globalSecondaryIndexes.toSeq: _*)
    }
    val builder3 = {
      if (localSecondaryIndexes.isEmpty) builder2
      else builder2.localSecondaryIndexes(localSecondaryIndexes.toSeq: _*)
    }
    createTable[F](builder3.build(), waitTillReady)(jClient)
  }

  private def createTable[F[_]: Async](
    createTableRequest: CreateTableRequest,
    waitTillReady: Boolean
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    val created = liftFuture(jClient.createTable(createTableRequest))
      .map(_.tableDescription().tableName())
    if (waitTillReady) {
      created.flatTap(name => waitTillActive(name)(jClient)).void
    } else {
      ().pure[F]
    }
  }

  private def waitTillActive[F[_]: Async](
    tableName: String,
    pollMs: FiniteDuration = 100.milliseconds
  )(jClient: DynamoDbAsyncClient): F[Unit] =
    describeOp(tableName)(jClient).flatMap { resp =>
      resp.tableStatus() match {
        case TableStatus.ACTIVE => ().pure[F]
        case TableStatus.CREATING | TableStatus.UPDATING =>
          Temporal[F].sleep(pollMs) >> waitTillActive(
            tableName,
            pollMs
          )(jClient)
        case status =>
          UnexpectedTableStatus(
            tableName,
            status,
            Set(TableStatus.ACTIVE)
          ).raiseError[F, Unit]
      }
    }

  private def dynamoKey(
    attributeName: String,
    keyType: KeyType
  ): KeySchemaElement =
    KeySchemaElement
      .builder()
      .attributeName(attributeName)
      .keyType(keyType)
      .build()

  private def dynamoAttribute(
    attributeName: String,
    attributeType: DynamoDbType
  ): AttributeDefinition =
    AttributeDefinition
      .builder()
      .attributeName(attributeName)
      .attributeType(attributeType.toScalarAttributeType)
      .build()
}
