package meteor

import cats.effect.Concurrent
import cats.implicits._
import fs2.{Pipe, RaiseThrowable}
import meteor.codec.{Decoder, DecoderFailure, Encoder}
import meteor.errors.InvalidExpression
import meteor.implicits._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.jdk.CollectionConverters._

class DefaultClient[F[_]: Concurrent: RaiseThrowable](
  jClient: DynamoDbAsyncClient
) extends Client[F] {

  def get[T: Decoder, P: Encoder](
    table: Table,
    partitionKey: P,
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
    table: Table,
    partitionKey: P,
    sortKey: S,
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
    table: Table,
    query: Query[P, S],
    consistentRead: Boolean,
    index: Option[Index] = None,
    limit: Int = Int.MaxValue
  ): fs2.Stream[F, T] =
    fs2.Stream.fromEither[F](
      Either.fromOption(
        query.keyCondition.nonEmpty.guard[Option].as(query.keyCondition),
        InvalidExpression
      )
    ).flatMap {
      cond =>
        val builder = {
          val builder0 =
            QueryRequest.builder()
              .tableName(table.name)
              .consistentRead(consistentRead)
              .keyConditionExpression(cond.expression)
              .limit(limit)
          val builder1 =
            index.fold(builder0)(index => builder0.indexName(index.name))
          if (query.filter.isEmpty) {
            builder1
              .expressionAttributeNames(cond.attributeNames.asJava)
              .expressionAttributeValues(cond.attributeValues.asJava)
          } else {
            builder1.filterExpression(
              query.filter.expression
            ).expressionAttributeNames(
              (cond.attributeNames ++ query.filter.attributeNames).asJava
            ).expressionAttributeValues(
              (cond.attributeValues ++ query.filter.attributeValues).asJava
            )
          }
        }

        def doQuery(
          req: QueryRequest
        ): fs2.Stream[F, QueryResponse] =
          fs2.Stream.eval((() => jClient.query(req)).liftF[F]).flatMap { resp =>
            if (resp.hasLastEvaluatedKey) {
              val nextReq =
                builder.exclusiveStartKey(resp.lastEvaluatedKey()).build()
              fs2.Stream.emit[F, QueryResponse](resp) ++ doQuery(nextReq)
            } else {
              fs2.Stream.emit[F, QueryResponse](resp)
            }
          }

        type FailureOr[U] = Either[DecoderFailure, U]

        for {
          resp <- doQuery(builder.build())
          listT <- fs2.Stream.fromEither(
            resp.items().asScala.toList.traverse[FailureOr, Option[T]](
              _.attemptDecode[T]
            ).map(
              _.flatten
            )
          )
          result <- fs2.Stream.emits(listT)
        } yield result
    }

  def put[T: Encoder](
    table: Table,
    t: T
  ): F[Unit] = put[T](table, t, Expression.empty)

  def put[T: Encoder](
    table: Table,
    t: T,
    condition: Expression
  ): F[Unit] = {
    val builder =
      PutItemRequest.builder()
        .tableName(table.name)
        .item(Encoder[T].write(t).m())
        .returnValues(ReturnValue.NONE)
    val req = condition.nonEmpty.guard[Option].as(condition).fold(builder) {
      cond =>
        builder
          .conditionExpression(cond.expression)
          .expressionAttributeNames(cond.attributeNames.asJava)
          .expressionAttributeValues(cond.attributeValues.asJava)
    }.build()
    (() => jClient.putItem(req)).liftF[F].void
  }

  def put[T: Encoder, U: Decoder](
    table: Table,
    t: T
  ): F[Option[U]] = put[T, U](table, t, Expression.empty)

  def put[T: Encoder, U: Decoder](
    table: Table,
    t: T,
    condition: Expression
  ): F[Option[U]] = {
    val builder =
      PutItemRequest.builder()
        .tableName(table.name)
        .item(Encoder[T].write(t).m())
        .returnValues(ReturnValue.ALL_OLD)
    val req = condition.nonEmpty.guard[Option].as(condition).fold(builder) {
      cond =>
        builder
          .conditionExpression(cond.expression)
          .expressionAttributeNames(cond.attributeNames.asJava)
          .expressionAttributeValues(cond.attributeValues.asJava)
    }.build()
    (() => jClient.putItem(req)).liftF[F].flatMap { resp =>
      Concurrent[F].fromEither(resp.attributes().attemptDecode[U])
    }
  }

  def delete[P: Encoder, S: Encoder](
    table: Table,
    partitionKey: P,
    sortKey: S
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

  def scan[T: Decoder](
    table: Table,
    filter: Expression,
    consistentRead: Boolean,
    parallelism: Int
  ): fs2.Stream[F, Option[T]] = {
    def requestBuilder(
      filter: Expression,
      startKey: Option[java.util.Map[String, AttributeValue]]
    ) = {
      def builder(filter: Expression) = {
        ScanRequest.builder()
          .tableName(table.name)
          .consistentRead(consistentRead)
          .filterExpression(filter.expression)
          .expressionAttributeNames(filter.attributeNames.asJava)
          .expressionAttributeValues(filter.attributeValues.asJava)
          .totalSegments(parallelism)
      }

      startKey.fold(builder(filter))(builder(filter).exclusiveStartKey)
    }

    def initRequests(filter: Expression) =
      fs2.Stream.emits[F, SegmentPassThrough[ScanRequest]](
        List.fill(parallelism)(
          requestBuilder(filter, None)
        ).zipWithIndex.map {
          case (builder, index) =>
            SegmentPassThrough(builder.segment(index).build(), index)
        }
      )

    def sendPipe(filter: Expression): Pipe[
      F,
      SegmentPassThrough[ScanRequest],
      SegmentPassThrough[ScanResponse]
    ] =
      _.mapAsyncUnordered(parallelism)(req => doScan(filter, req)).parJoin(
        parallelism
      )

    def doScan(
      filter: Expression,
      req: SegmentPassThrough[ScanRequest]
    ): F[fs2.Stream[F, SegmentPassThrough[ScanResponse]]] = {
      (() => jClient.scan(req.u)).liftF[F].flatMap { resp =>
        if (resp.hasLastEvaluatedKey) {
          doScan(
            filter,
            SegmentPassThrough(
              requestBuilder(filter, Some(resp.lastEvaluatedKey())).segment(
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
        Concurrent[F].fromOption(
          filter.nonEmpty.guard[Option].as(filter),
          InvalidExpression
        )
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
