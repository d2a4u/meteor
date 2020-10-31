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
  def describeOp[F[_]: Concurrent](table: Table)(jClient: DynamoDbAsyncClient)
    : F[TableDescription] = {
    val req = DescribeTableRequest.builder().tableName(table.name).build()
    (() => jClient.describeTable(req)).liftF[F].map { resp =>
      resp.table()
    }
  }

  def deleteTableOp[F[_]: Concurrent](
    table: Table
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    val deleteTableRequest =
      DeleteTableRequest.builder().tableName(table.name).build()
    (() => jClient.deleteTable(deleteTableRequest)).liftF[F].void
  }

  def createTableOp[F[_]: Concurrent: Timer](
    table: Table,
    keys: Map[String, (KeyType, ScalarAttributeType)],
    billingMode: BillingMode,
    waitTillReady: Boolean
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    val createTableRequest =
      CreateTableRequest
        .builder()
        .tableName(table.name)
        .billingMode(billingMode)

    val keySchema = keys.map {
      case (k, v) => dynamoKey(k, v._1)
    }

    val keyAttr = keys.map {
      case (k, v) => dynamoAttribute(k, v._2)
    }

    createTableRequest
      .keySchema(keySchema.toSeq: _*)
      .attributeDefinitions(keyAttr.toSeq: _*)
    createTable[F](createTableRequest.build(), waitTillReady)(jClient)
  }

  private def createTable[F[_]: Concurrent: Timer](
    createTableRequest: CreateTableRequest,
    waitTillReady: Boolean
  )(jClient: DynamoDbAsyncClient): F[Unit] = {
    val created = (() => jClient.createTable(createTableRequest)).liftF[F]
      .map(_.tableDescription().tableName())
    if (waitTillReady) {
      created.flatTap(name => waitTillActive(Table(name))(jClient)).void
    } else {
      ().pure[F]
    }
  }

  private def waitTillActive[F[_]: Concurrent: Timer](
    table: Table,
    pollMs: FiniteDuration = 100.milliseconds
  )(jClient: DynamoDbAsyncClient): F[Unit] =
    describeOp(table)(jClient).flatMap { resp =>
      resp.tableStatus() match {
        case TableStatus.ACTIVE => ().pure[F]
        case TableStatus.CREATING | TableStatus.UPDATING =>
          Timer[F].sleep(pollMs) >> waitTillActive(
            table,
            pollMs
          )(jClient)
        case status =>
          UnexpectedTableStatus(
            table,
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
