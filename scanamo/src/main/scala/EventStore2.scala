//package meteor
//
//import java.util.{Map => JMap}
//
//import cats.effect._
//import fs2.{Pull, Stream}
//import meteor.implicits._
//import org.scanamo.DynamoFormat
//import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
//import software.amazon.awssdk.services.dynamodb.model.{
//  AttributeValue,
//  ScanRequest,
//  ScanResponse
//}
//
//import scala.jdk.CollectionConverters._
//
//class EventStore2[F[_]: Concurrent: ContextShift: Timer](
//  client: DynamoDbAsyncClient,
//  tableName: String
//) {
//  type PageKey = JMap[String, AttributeValue]
//  val total = 32
//
//  def scan[V: DynamoFormat](consistent: Boolean = false): Stream[F, PageKey] = {
//    def call(segment: Int, lastEvaluatedKey: PageKey): F[ScanResponse] =
//      Sync[F].suspend {
//        val req = ScanRequest.builder()
//          .tableName(tableName)
//          .totalSegments(total)
//          .segment(segment)
//          .consistentRead(consistent)
//        if (!lastEvaluatedKey.isEmpty)
//          req.exclusiveStartKey(lastEvaluatedKey)
//        (() => client.scan(req.build())).liftF2[F]
//      }
//
//    def output(r: ScanResponse) =
//      Stream.emits[F, JMap[String, AttributeValue]](r.items().asScala.toList)
//
//    // Java weirdness. Key not present might mean null or empty
//    def nextInput(r: ScanResponse): Option[PageKey] =
//      Option(r.lastEvaluatedKey).filter(!_.isEmpty)
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
//}
