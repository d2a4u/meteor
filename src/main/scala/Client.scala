package meteor

import java.util.concurrent.Executor

import cats.effect.{Concurrent, Resource, Sync}
import cats.implicits._
import meteor.codec.{Decoder, Encoder}
import meteor.errors.InvalidCondition
import meteor.implicits._
import software.amazon.awssdk.auth.credentials.{
  AwsCredentialsProviderChain,
  DefaultCredentialsProvider
}
import software.amazon.awssdk.core.client.config.{
  ClientAsyncConfiguration,
  SdkAdvancedAsyncClientOption
}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.jdk.CollectionConverters._

sealed trait Client[F[_]] {

  def get[T: Decoder, P: Encoder, S: Encoder](
    partitionKey: P,
    sortKey: S,
    tableName: TableName,
    consistentRead: Boolean
  ): F[Option[T]]

  def retrieve[T: Decoder, P: Encoder, S: Encoder](
    query: Query[P, S],
    tableName: TableName,
    consistentRead: Boolean
  ): F[List[T]]

  def put[T: Encoder, U: Decoder](
    t: T,
    tableName: TableName,
    returnValue: PutItemReturnValue = PutItemReturnValue.None
  ): F[Option[U]]

  def delete[P: Encoder, S: Encoder](
    partitionKey: P,
    sortKey: S,
    tableName: TableName
  ): F[Unit]
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

class DefaultClient[F[_]: Concurrent](jClient: DynamoDbAsyncClient)
    extends Client[F] {
  def get[T: Decoder, P: Encoder, S: Encoder](
    partitionKey: P,
    sortKey: S,
    tableName: TableName,
    consistentRead: Boolean
  ): F[Option[T]] = {
    val query = Encoder[P].write(partitionKey).m().asScala ++ Encoder[S].write(
      sortKey
    ).m().asScala
    val req = GetItemRequest.builder().consistentRead(consistentRead).tableName(
      tableName.value
    ).key(query.asJava).build()
    (() => jClient.getItem(req)).liftF[F].flatMap { resp =>
      Concurrent[F].fromEither(resp.item().attemptDecode[T])
    }
  }

  def retrieve[T: Decoder, P: Encoder, S: Encoder](
    query: Query[P, S],
    tableName: TableName,
    consistentRead: Boolean
  ): F[List[T]] = {
    Concurrent[F].fromOption(query.condition, InvalidCondition).flatMap {
      cond =>
        val req =
          QueryRequest.builder().tableName(
            tableName.value
          ).keyConditionExpression(cond.expression).expressionAttributeValues(
            cond.attributes.asJava
          ).build()
        (() => jClient.query(req)).liftF[F].map { resp =>
          resp.items().asScala.toList.traverse(_.attemptDecode[T]).map(
            _.flatten
          )
        }.flatMap(Concurrent[F].fromEither)
    }
  }

  def put[T: Encoder, U: Decoder](
    t: T,
    tableName: TableName,
    returnValue: PutItemReturnValue = PutItemReturnValue.None
  ): F[Option[U]] = {
    val returnVal = returnValue match {
      case PutItemReturnValue.None => ReturnValue.NONE
      case PutItemReturnValue.AllOld => ReturnValue.ALL_OLD
    }
    val req = PutItemRequest.builder().tableName(tableName.value).item(
      Encoder[T].write(t).m()
    ).returnValues(returnVal).build()
    (() => jClient.putItem(req)).liftF[F].flatMap { resp =>
      returnValue match {
        case PutItemReturnValue.None => none[U].pure[F]
        case PutItemReturnValue.AllOld =>
          Concurrent[F].fromEither(resp.attributes().attemptDecode[U])
      }
    }
  }

  def delete[P: Encoder, S: Encoder](
    partitionKey: P,
    sortKey: S,
    tableName: TableName
  ): F[Unit] = {
    val req =
      DeleteItemRequest.builder().tableName(
        tableName.value
      ).key((Encoder[P].write(partitionKey).m().asScala ++ Encoder[S].write(
        sortKey
      ).m().asScala).asJava).build()
    (() => jClient.deleteItem(req)).liftF[F].void
  }
}
