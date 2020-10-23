package meteor
package api

import cats.effect.Concurrent
import cats.implicits._
import meteor.implicits._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

trait DescribeOps {
  def describeOp[F[_]: Concurrent](table: Table)(jClient: DynamoDbAsyncClient)
    : F[TableDescription] = {
    val req = DescribeTableRequest.builder().tableName(table.name).build()
    (() => jClient.describeTable(req)).liftF[F].map { resp =>
      resp.table()
    }
  }
}
