//package meteor
//package eventstore
//
//import java.util.concurrent.{Future => JFuture}
//import java.util.{Map => JMap}
//
//import cats.effect._
//import cats.effect.implicits._
//import cats.implicits._
//import com.amazonaws.AmazonWebServiceRequest
//import com.amazonaws.handlers.AsyncHandler
//import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync
//import com.amazonaws.services.dynamodbv2.model._
//import fs2.{Pull, Stream}
//import org.scanamo.DynamoFormat
//
//import scala.jdk.CollectionConverters._
//
//class EventStore[F[_]: Concurrent: ContextShift: Timer](
//  client: AmazonDynamoDBAsync,
//  tableName: String
//) {
//  type PageKey = JMap[String, AttributeValue]
//  val total = 32
//
//  def scan[V: DynamoFormat](consistent: Boolean = false): Stream[F, PageKey] = {
//    def call(segment: Int, lastEvaluatedKey: PageKey): F[ScanResult] =
//      Sync[F].suspend {
//        val req = new ScanRequest(tableName)
//          .withTotalSegments(total)
//          .withSegment(segment)
//          .withConsistentRead(consistent)
//        if (!lastEvaluatedKey.isEmpty)
//          req.setExclusiveStartKey(lastEvaluatedKey)
//        submit(req)(client.scanAsync)
//      }
//    def output(r: ScanResult) =
//      Stream.emits[F, JMap[String, AttributeValue]](r.getItems.asScala.toList)
//
//    // Java weirdness. Key not present might mean null or empty
//    def nextInput(r: ScanResult): Option[PageKey] =
//      Option(r.getLastEvaluatedKey).filter(!_.isEmpty)
//    Stream
//      .range(0, total)
//      .map { s =>
//        paginated(
//          Map.empty[String, AttributeValue].asJava: PageKey,
//          call(s, _),
//          output,
//          nextInput
//        ).flatten
//      }
//      .parJoinUnbounded
//  }
//  def paginated[I, O, A](
//    seed: I,
//    call: I => F[O],
//    output: O => A,
//    nextInput: O => Option[I]
//  ): Stream[F, A] =
//    Pull
//      .loop {
//        in: I =>
//          Pull.eval(call(in)).flatMap { r =>
//            Pull.output1(output(r)).as(nextInput(r))
//          }
//      }
//      .apply(seed)
//      .void
//      .stream
//
//  def submit[Req <: AmazonWebServiceRequest, Res](
//    req: Req
//  )(dynamoApi: (Req, AsyncHandler[Req, Res]) => JFuture[Res]): F[Res] = {
//    Async[F]
//      .async[Res] { cb =>
//        val handler = new AsyncHandler[Req, Res] {
//          def onError(exception: Exception): Unit = cb(exception.asLeft)
//          def onSuccess(request: Req, result: Res): Unit =
//            cb(result.asRight)
//        }
//        dynamoApi(req, handler)
//        ()
//      }
//      .guarantee(ContextShift[F].shift)
//  }
//}
