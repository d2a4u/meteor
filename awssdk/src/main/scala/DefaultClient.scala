package meteor

import cats.effect.Concurrent
import cats.implicits._
import fs2.{Pipe, RaiseThrowable}
import meteor.codec.{Decoder, Encoder}
import meteor.errors.InvalidCondition
import meteor.implicits._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.jdk.CollectionConverters._

class DefaultClient[F[_]: Concurrent: RaiseThrowable](
  jClient: DynamoDbAsyncClient
) extends Client[F] {

  def get[T: Decoder, P: Encoder](
    partitionKey: P,
    table: Table,
    consistentRead: Boolean
  ): F[Option[T]] = {
    val query = Encoder[P].write(partitionKey).m()
    val req =
      GetItemRequest.builder()
        .consistentRead(consistentRead)
        .tableName(table.name)
        .key(query)
        .build()
    (() => jClient.getItem(req)).liftF[F].flatMap { resp =>
      Concurrent[F].fromEither(resp.item().attemptDecode[T])
    }
  }

  def get[T: Decoder, P: Encoder, S: Encoder](
    partitionKey: P,
    sortKey: S,
    table: Table,
    consistentRead: Boolean
  ): F[Option[T]] = {
    val query = Encoder[P].write(partitionKey).m().asScala ++ Encoder[S].write(
      sortKey
    ).m().asScala
    val req =
      GetItemRequest.builder()
        .consistentRead(consistentRead)
        .tableName(table.name)
        .key(query.asJava)
        .build()
    (() => jClient.getItem(req)).liftF[F].flatMap { resp =>
      Concurrent[F].fromEither(resp.item().attemptDecode[T])
    }
  }

  def retrieve[T: Decoder, P: Encoder, S: Encoder](
    query: Query[P, S],
    table: Table,
    consistentRead: Boolean,
    index: Option[Index]
  ): F[List[T]] = {
    Concurrent[F].fromOption(query.condition, InvalidCondition).flatMap {
      cond =>
        val builder =
          QueryRequest.builder()
            .tableName(table.name)
            .consistentRead(consistentRead)
            .keyConditionExpression(cond.expression)
            .expressionAttributeValues(cond.attributes.asJava)
        val req =
          index.fold(builder)(index => builder.indexName(index.name)).build()
        (() => jClient.query(req)).liftF[F].map { resp =>
          resp.items().asScala.toList.traverse(_.attemptDecode[T]).map(
            _.flatten
          )
        }.flatMap(Concurrent[F].fromEither)
    }
  }

  def put[T: Encoder](t: T, table: Table): F[Unit] = {
    val req =
      PutItemRequest.builder()
        .tableName(table.name)
        .item(Encoder[T].write(t).m())
        .returnValues(ReturnValue.NONE)
        .build()
    (() => jClient.putItem(req)).liftF[F].void
  }

  def put[T: Encoder, U: Decoder](t: T, table: Table): F[Option[U]] = {
    val req =
      PutItemRequest.builder()
        .tableName(table.name)
        .item(Encoder[T].write(t).m())
        .returnValues(ReturnValue.ALL_OLD)
        .build()
    (() => jClient.putItem(req)).liftF[F].flatMap { resp =>
      Concurrent[F].fromEither(resp.attributes().attemptDecode[U])
    }
  }

  def delete[P: Encoder, S: Encoder](
    partitionKey: P,
    sortKey: S,
    table: Table
  ): F[Unit] = {
    val req =
      DeleteItemRequest.builder()
        .tableName(table.name)
        .key((Encoder[P].write(partitionKey).m().asScala ++ Encoder[S].write(
          sortKey
        ).m().asScala).asJava)
        .build()
    (() => jClient.deleteItem(req)).liftF[F].void
  }

  private case class SegmentPassThrough[U](
    u: U,
    segment: Int
  )

  def scan[T: Decoder, P: Encoder, S: Encoder](
    query: Query[P, S],
    table: Table,
    consistentRead: Boolean,
    parallelism: Int
  ): fs2.Stream[F, Option[T]] = {
    def requestBuilder(
      cond: meteor.Condition,
      startKey: Option[java.util.Map[String, AttributeValue]]
    ) = {
      def builder(cond: meteor.Condition) = {
        ScanRequest.builder()
          .tableName(table.name)
          .consistentRead(consistentRead)
          .filterExpression(cond.expression)
          .expressionAttributeValues(cond.attributes.asJava)
          .totalSegments(parallelism)
      }

      startKey.fold(builder(cond))(builder(cond).exclusiveStartKey)
    }

    def initRequests(cond: meteor.Condition) =
      fs2.Stream.emits[F, SegmentPassThrough[ScanRequest]](
        List.fill(parallelism)(
          requestBuilder(cond, None)
        ).zipWithIndex.map {
          case (builder, index) =>
            SegmentPassThrough(builder.segment(index).build(), index)
        }
      )

    def sendPipe(cond: meteor.Condition): Pipe[
      F,
      SegmentPassThrough[ScanRequest],
      SegmentPassThrough[ScanResponse]
    ] =
      _.mapAsyncUnordered(parallelism)(req => doScan(cond, req)).parJoin(
        parallelism
      )

    def doScan(
      cond: meteor.Condition,
      req: SegmentPassThrough[ScanRequest]
    ): F[fs2.Stream[F, SegmentPassThrough[ScanResponse]]] = {
      (() => jClient.scan(req.u)).liftF[F].flatMap { resp =>
        if (resp.hasLastEvaluatedKey) {
          doScan(
            cond,
            SegmentPassThrough(
              requestBuilder(cond, Some(resp.lastEvaluatedKey())).segment(
                req.segment
              ).build(),
              req.segment
            )
          ).map { stream =>
            fs2.Stream.emit(SegmentPassThrough(resp, req.segment)) ++ stream
          }
        } else {
          fs2.Stream.emit[F, SegmentPassThrough[ScanResponse]](
            SegmentPassThrough(resp, req.segment)
          ).pure[F]
        }
      }
    }

    for {
      cond <- fs2.Stream.eval(
        Concurrent[F].fromOption(query.condition, InvalidCondition)
      )
      resp <- sendPipe(cond)(initRequests(cond))
      attrs <- fs2.Stream.emits(resp.u.items().asScala.toList)
      optT <- fs2.Stream.fromEither(attrs.attemptDecode[T])
    } yield optT
  }

  def scan[T: Decoder](
    table: Table,
    consistentRead: Boolean,
    parallelism: Int
  ): fs2.Stream[F, Option[T]] = {

    def requestBuilder(
      startKey: Option[java.util.Map[String, AttributeValue]]
    ) = {
      val builder =
        ScanRequest.builder()
          .tableName(table.name)
          .consistentRead(consistentRead)
          .totalSegments(parallelism)

      startKey.fold(builder)(builder.exclusiveStartKey)
    }

    lazy val initRequests =
      fs2.Stream.emits[F, SegmentPassThrough[ScanRequest]](
        List.fill(parallelism)(
          requestBuilder(None)
        ).zipWithIndex.map {
          case (builder, index) =>
            SegmentPassThrough(builder.segment(index).build(), index)
        }
      )

    lazy val sendPipe: Pipe[
      F,
      SegmentPassThrough[ScanRequest],
      SegmentPassThrough[ScanResponse]
    ] =
      _.mapAsyncUnordered(parallelism)(doScan).parJoin(parallelism)

    def doScan(
      req: SegmentPassThrough[ScanRequest]
    ): F[fs2.Stream[F, SegmentPassThrough[ScanResponse]]] = {
      (() => jClient.scan(req.u)).liftF[F].flatMap { resp =>
        if (resp.hasLastEvaluatedKey) {
          doScan(
            SegmentPassThrough(
              requestBuilder(Some(resp.lastEvaluatedKey())).segment(
                req.segment
              ).build(),
              req.segment
            )
          ).map { stream =>
            fs2.Stream.emit(SegmentPassThrough(resp, req.segment)) ++ stream
          }
        } else {
          fs2.Stream.emit[F, SegmentPassThrough[ScanResponse]](
            SegmentPassThrough(resp, req.segment)
          ).pure[F]
        }
      }
    }

    for {
      resp <- sendPipe(initRequests)
      attrs <- fs2.Stream.emits(resp.u.items().asScala.toList)
      optT <- fs2.Stream.fromEither(attrs.attemptDecode[T])
    } yield optT
  }

  def describe(table: Table): F[TableDescription] = {
    val req = DescribeTableRequest.builder().tableName(table.name).build()
    (() => jClient.describeTable(req)).liftF[F].map { resp =>
      resp.table()
    }
  }
}
