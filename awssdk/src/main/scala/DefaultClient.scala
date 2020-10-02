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

  def scan[T: Decoder, P: Encoder, S: Encoder](
    query: Query[P, S],
    tableName: TableName,
    consistentRead: Boolean,
    parallelism: Int
  ): fs2.Stream[F, Option[T]] = {
    case class SegmentPassThrough[U](
      u: U,
      segment: Int
    )

    def requestBuilder(
      cond: meteor.Condition,
      startKey: Option[java.util.Map[String, AttributeValue]]
    ) = {
      def builder(cond: meteor.Condition) = {
        ScanRequest.builder().tableName(tableName.value).consistentRead(
          consistentRead
        ).filterExpression(cond.expression).expressionAttributeValues(
          cond.attributes.asJava
        ).totalSegments(parallelism)
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
      in => {
        val segmentResponses = in.mapAsync(parallelism) { req =>
          (() => jClient.scan(req.u)).liftF[F].map(resp =>
            SegmentPassThrough(resp, req.segment))
        }
        segmentResponses ++ segmentResponses.flatMap { resp =>
          doScan(
            cond,
            SegmentPassThrough(
              requestBuilder(cond, Some(resp.u.lastEvaluatedKey())).segment(
                resp.segment
              ).build(),
              resp.segment
            )
          )
        }
      }

    def doScan(
      cond: meteor.Condition,
      req: SegmentPassThrough[ScanRequest]
    ): fs2.Stream[F, SegmentPassThrough[ScanResponse]] = {
      val respF = (() => jClient.scan(req.u)).liftF[F]
      fs2.Stream.eval(respF).flatMap { resp =>
        if (resp.hasLastEvaluatedKey) {
          fs2.Stream.emit(SegmentPassThrough(resp, req.segment)) ++
            doScan(
              cond,
              SegmentPassThrough(
                requestBuilder(cond, Some(resp.lastEvaluatedKey())).segment(
                  req.segment
                ).build(),
                req.segment
              )
            )
        } else {
          fs2.Stream.empty
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
}
