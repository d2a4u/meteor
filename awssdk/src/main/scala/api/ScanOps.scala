package meteor
package api

import cats.effect.Concurrent
import cats.implicits._
import fs2.{Pipe, RaiseThrowable}
import meteor.codec.Decoder
import meteor.errors.InvalidExpression
import meteor.implicits._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.jdk.CollectionConverters._

trait ScanOps {

  def scanOp[F[_]: Concurrent: RaiseThrowable, T: Decoder](
    table: Table,
    filter: Expression,
    consistentRead: Boolean,
    parallelism: Int
  )(jClient: DynamoDbAsyncClient): fs2.Stream[F, Option[T]] = {
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

  def scanOp[F[_]: Concurrent: RaiseThrowable, T: Decoder](
    table: Table,
    consistentRead: Boolean,
    parallelism: Int
  )(jClient: DynamoDbAsyncClient): fs2.Stream[F, Option[T]] = {

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

  private case class SegmentPassThrough[U](
    u: U,
    segment: Int
  )
}
