package meteor
package api

import cats.effect.{Concurrent, Timer}
import cats.implicits._
import meteor.errors.UnexpectedTableStatus
import meteor.implicits._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.concurrent.duration.{DurationInt, FiniteDuration}

private[meteor] trait TableOps {
  private[meteor] def describeOp[F[_]: Concurrent](tableName: String)(
    jClient: DynamoDbAsyncClient
  ): F[TableDescription] = {
    val req = DescribeTableRequest.builder().tableName(tableName).build()
    (() => jClient.describeTable(req)).liftF[F].map { resp =>
      resp.table()
    }
  }

  private[meteor] def deleteTableOp[F[_]: Concurrent](
    tableName: String
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    val deleteTableRequest =
      DeleteTableRequest.builder().tableName(tableName).build()
    (() => jClient.deleteTable(deleteTableRequest)).liftF[F].void
  }

  private[meteor] def createCompositeKeysTableOp[
    F[_]: Concurrent: Timer,
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

  private[meteor] def createPartitionKeyTableOp[
    F[_]: Concurrent: Timer,
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
    F[_]: Concurrent: Timer
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

  private def createTable[F[_]: Concurrent: Timer](
    createTableRequest: CreateTableRequest,
    waitTillReady: Boolean
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    val created = (() => jClient.createTable(createTableRequest)).liftF[F]
      .map(_.tableDescription().tableName())
    if (waitTillReady) {
      created.flatTap(name => waitTillActive(name)(jClient)).void
    } else {
      ().pure[F]
    }
  }

  private def waitTillActive[F[_]: Concurrent: Timer](
    tableName: String,
    pollMs: FiniteDuration = 100.milliseconds
  )(jClient: DynamoDbAsyncClient): F[Unit] =
    describeOp(tableName)(jClient).flatMap { resp =>
      resp.tableStatus() match {
        case TableStatus.ACTIVE => ().pure[F]
        case TableStatus.CREATING | TableStatus.UPDATING =>
          Timer[F].sleep(pollMs) >> waitTillActive(
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
