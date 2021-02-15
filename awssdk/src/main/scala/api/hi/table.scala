//package meteor
//package api.hi
//
//import cats.MonadError
//import cats.effect.Concurrent
//import cats.implicits._
//import meteor.codec._
//import meteor.{SecondaryIndex => LoSecondaryIndex}
//import meteor.errors.EncoderError
//import meteor.implicits._
//import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
//import software.amazon.awssdk.services.dynamodb.model.{
//  AttributeValue,
//  GetItemRequest
//}
//
//import scala.jdk.CollectionConverters._
//
//case class PartitionKeyTable[F[_]: Concurrent, P: Encoder](
//  tableName: String,
//  partitionKeyDef: IndexKey[P],
//  client: DynamoDbAsyncClient
//) extends Index {
//  def get[U: Decoder](
//    partitionKey: P,
//    consistentRead: Boolean
//  ): F[Option[U]] = {
//    partitionKeyDef.make[F](partitionKey).flatMap { pk =>
//      val req =
//        GetItemRequest.builder()
//          .consistentRead(consistentRead)
//          .tableName(tableName)
//          .key(pk)
//          .build()
//      (() => client.getItem(req)).liftF[F].flatMap { resp =>
//        if (resp.hasItem()) {
//          Concurrent[F].fromEither(
//            resp.item().asAttributeValue.as[U].map(_.some).leftWiden[Throwable]
//          )
//        } else {
//          none[U].pure[F]
//        }
//      }
//    }
//  }
//}
//
//abstract class SharedOps[F[_]](tableName: String, client: Client[F]) {
//  def put[T: Encoder](t: T): F[Unit] = client.put(tableName, t)
//}
//
//sealed trait CompositeKeysIndex extends Index
//
//case class CompositeKeysTable[F[_], P: Encoder, S: Encoder](
//  tableName: String,
//  partitionKey: IndexKey[P],
//  sortKey: IndexKey[S],
//  client: Client[F]
//) extends SharedOps[F](tableName, client)
//    with CompositeKeysIndex {
//  private val table = Table(
//    tableName,
//    KeyDef(partitionKey.attributeName, partitionKey.attributeType),
//    Some(KeyDef(sortKey.attributeName, sortKey.attributeType))
//  )
//  def get[U: Decoder](
//    partitionKey: P,
//    sortKey: S,
//    consistentRead: Boolean
//  ): F[Option[U]] =
//    client.get[P, S, U](table, partitionKey, sortKey, consistentRead)
//
//  def retrieve[U: Decoder](
//    query: Query[P, S],
//    consistentRead: Boolean,
//    limit: Int
//  ): fs2.Stream[F, U] =
//    client.retrieve[P, S, U](table, query, consistentRead, limit)
//}
//
//case class SecondaryIndex[F[_], P: Encoder, S: Encoder](
//  tableName: String,
//  indexName: String,
//  partitionKey: IndexKey[P],
//  sortKey: IndexKey[S]
//) extends CompositeKeysIndex {
//  private val index = LoSecondaryIndex(
//    tableName,
//    indexName,
//    partitionKey,
//    Some(sortKey)
//  )
//  def retrieve[P: Encoder, S: Encoder, U: Decoder](
//    query: Query[P, S],
//    consistentRead: Boolean,
//    limit: Int
//  ): fs2.Stream[F, U]
//}
//
////sealed trait DynamoIndex[P, S] {
////  def name: String
////}
////case class IndexKey[K: Encoder](
////  attributeName: String,
////  attributeType: DynamoDbType
////) {
////  def toAttributeValue(k: K): AttributeValue =
////    Map(attributeName -> k.asAttributeValue).asAttributeValue
////}
////
////case class PartitionKeyTable[F[_], P: Encoder](
////  tableName: String,
////  partitionKey: IndexKey[P],
////  client: Client[F]
////) extends PartitionKeyOps[F, P] with DynamoIndex[P, Nothing] {
////  def get[U: Decoder](
////    partitionKey: P,
////    consistentRead: Boolean
////  ): F[Option[U]] = {
////    ???
////  }
////}
////
////case class CompositeKeysTable[F[_], P: Encoder, S: Encoder](
////  tableName: String,
////  partitionKey: IndexKey[P],
////  sortKey: IndexKey[S]
////) extends DynamoIndex[P, S]
////    with CompositeKeysOps[F]
////
////case class CompositeKeysIndex[F[_], P: Encoder, S: Encoder](
////  tableName: String,
////  indexName: String,
////  partitionKey: IndexKey[P],
////  sortKey: IndexKey[S]
////) extends
////
////abstract class PartitionKeyOps[F[_], P: Encoder] extends SharedOps[F] {
////
////  /**
////    * Get a single value from a table by partition key P.
////    */
////  def get[U: Decoder](
////    partitionKey: P,
////    consistentRead: Boolean
////  ): F[Option[U]]
////}
////
////trait CompositeKeysOps[F[_]] extends SharedOps[F] {}
////
////trait SharedOps[F[_]] {}
