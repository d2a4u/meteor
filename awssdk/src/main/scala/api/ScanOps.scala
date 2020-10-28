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
  )(jClient: DynamoDbAsyncClient): fs2.Stream[F, T] = {
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
      _.map(req => loop(filter, req)).parJoin(parallelism)

    def loop(
      filter: Expression,
      req: SegmentPassThrough[ScanRequest]
    ): fs2.Stream[F, SegmentPassThrough[ScanResponse]] = {
      fs2.Stream.eval((() => jClient.scan(req.u)).liftF[F]).map { resp =>
        if (resp.hasLastEvaluatedKey) {
          val next = loop(
            filter,
            SegmentPassThrough(
              requestBuilder(filter, Some(resp.lastEvaluatedKey())).segment(
                req.segment
              ).build(),
              req.segment
            )
          )
          fs2.Stream.emit(SegmentPassThrough(resp, req.segment)) ++ next
        } else {
          fs2.Stream.emit[F, SegmentPassThrough[ScanResponse]](
            SegmentPassThrough(resp, req.segment)
          )
        }
      }.parJoin(parallelism)
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
      t <- fs2.Stream.fromEither(attrs.attemptDecode[T]).collect {
        case Some(t) => t
      }
    } yield t
  }

  def scanOp[F[_]: Concurrent: RaiseThrowable, T: Decoder](
    table: Table,
    consistentRead: Boolean,
    parallelism: Int
  )(jClient: DynamoDbAsyncClient): fs2.Stream[F, T] = {

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
      fs2.Stream.range(0, parallelism).map { index =>
        val builder = requestBuilder(None)
        SegmentPassThrough(builder.segment(index).build(), index)
      }

    def loop(
      req: SegmentPassThrough[ScanRequest]
    ): fs2.Stream[F, SegmentPassThrough[ScanResponse]] = {
      fs2.Stream.eval((() => jClient.scan(req.u)).liftF[F]).map { resp =>
        if (resp.hasLastEvaluatedKey) {
          val next = loop(
            SegmentPassThrough(
              requestBuilder(Some(resp.lastEvaluatedKey())).segment(
                req.segment
              ).build(),
              req.segment
            )
          )
          fs2.Stream.emit(SegmentPassThrough(resp, req.segment)) ++ next
        } else {
          fs2.Stream.emit[F, SegmentPassThrough[ScanResponse]](
            SegmentPassThrough(resp, req.segment)
          )
        }
      }.parJoin(parallelism)
    }

    val sendPipe: Pipe[
      F,
      SegmentPassThrough[ScanRequest],
      SegmentPassThrough[ScanResponse]
    ] =
      _.map(loop).parJoin(parallelism)

    for {
      resp <- sendPipe(initRequests)
      attrs <- fs2.Stream.emits(resp.u.items().asScala.toList)
      t <- fs2.Stream.fromEither(attrs.attemptDecode[T]).collect {
        case Some(t) => t
      }
    } yield t
  }

  private case class SegmentPassThrough[U](
    u: U,
    segment: Int
  )
}
