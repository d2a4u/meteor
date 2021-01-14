package meteor
package api

import cats.effect.{Concurrent, Timer}
import cats.implicits._
import meteor.errors.UnexpectedTableStatus
import meteor.implicits._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.concurrent.duration.{DurationInt, FiniteDuration}

trait TableOps {
  def describeOp[F[_]: Concurrent](tableName: String)(
    jClient: DynamoDbAsyncClient
  ): F[TableDescription] = {
    val req = DescribeTableRequest.builder().tableName(tableName).build()
    (() => jClient.describeTable(req)).liftF[F].map { resp =>
      resp.table()
    }
  }

  def deleteTableOp[F[_]: Concurrent](
    tableName: String
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    val deleteTableRequest =
      DeleteTableRequest.builder().tableName(tableName).build()
    (() => jClient.deleteTable(deleteTableRequest)).liftF[F].void
  }

  def createTableOp[F[_]: Concurrent: Timer](
    table: Table,
    billingMode: BillingMode,
    waitTillReady: Boolean
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    val createTableRequest =
      CreateTableRequest
        .builder()
        .tableName(table.name)
        .billingMode(billingMode)

    val hashKeySchema = List(dynamoKey(table.partitionKey.name, KeyType.HASH))
    val hashKeyAttr =
      List(dynamoAttribute(
        table.partitionKey.name,
        table.partitionKey.attributeType.toScalarAttributeType
      ))
    val rangeKeySchema =
      table.sortKey.fold(List.empty[KeySchemaElement])(key =>
        List(dynamoKey(key.name, KeyType.RANGE)))
    val rangeKeyAttr =
      table.sortKey.fold(List.empty[AttributeDefinition])(key =>
        List(dynamoAttribute(
          key.name,
          key.attributeType.toScalarAttributeType
        )))
    val keySchema = hashKeySchema ++ rangeKeySchema
    val keyAttr = hashKeyAttr ++ rangeKeyAttr
    createTableRequest
      .keySchema(keySchema: _*)
      .attributeDefinitions(keyAttr: _*)
    createTable[F](createTableRequest.build(), waitTillReady)(jClient)
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
    attributeType: ScalarAttributeType
  ): AttributeDefinition =
    AttributeDefinition
      .builder()
      .attributeName(attributeName)
      .attributeType(attributeType)
      .build()
}
