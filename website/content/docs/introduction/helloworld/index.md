---
title: "Hello World"
description: ""
lead: ""
date: 2021-01-26T22:25:49Z
lastmod: 2021-01-26T22:25:49Z
draft: false
images: []
menu:
  docs:
    parent: "introduction"
weight: 901
toc: true
---

Quick complete example to write and read from a DynamoDB table.

## Define Codec Type Classes

Codec (Encoder and Decoder) type classes are required to write and read a value of type `T` to and
from Java's `AttributeValue`.

```scala
import meteor._
import meteor.codec._
import meteor.syntax._

case class Book(id: Int, content: String)
object Book {
  implicit val bookEncoder: Encoder[Book] = Encoder.instance { book =>
    Map(
      "id" -> book.id.asAttributeValue,
      "content" -> book.content.asAttributeValue
    ).asAttributeValue
  }
  implicit val bookDecoder: Decoder[Book] = Decoder.instance { attributeValue =>
    for {
      id <- attributeValue.getAs[Int]("id")
      content <- attributeValue.getAs[String]("content")
    } yield Book(id, content)
  }
}
```

## Client and Table

#### Client

A `meteor` client is required to perform DynamoDB actions. To create a cats-effect `Resource` of
`Client`:

```scala
import cats.effect.Resource
import meteor._

val clientSrc: Resource[IO, Client[IO]] = Client.resource[IO]
```

Internally, a default Java `DynamoDbAsyncClient` is created. Alternatively, you can also inject a
`DynamoDbAsyncClient` via `Client.apply` method. Please note that `meteor`'s `Client` is considered
low level API since it is very similar to Java API, consider using the high level API below.

#### Table and Secondary Index

DynamoDB actions can be performed against a table or a secondary index of a table. To do so in
`meteor`, the table or secondary index needs to be explicitly defined such as:

```scala
import meteor.api.hi._

val jClientSrc = 
  Resource.fromAutoCloseable[F, DynamoDbAsyncClient] {
    Sync[F].delay {
      val cred = DefaultCredentialsProvider.create()
      DynamoDbAsyncClient.builder()
        .credentialsProvider(cred)
        .region(Region.EU_WEST_1)
        .build()
    }
  }

val tableSrc = 
  jClientSrc.map { jClient =>
    SimpleTable[F, Int]("books-table", KeyDef[Int]("id", DynamoDbType.N), jClient)
  }
```

All supported index types are:

- `SimpleTable[F[_], P]` - represent a table with partition key (no sort key)
- `SecondarySimpleIndex[F[_], P]` - represent a secondary index with partition key (no sort key)
- `CompositeTable[F[_], P, S]` - represent a table with partition key and sort key
- `SecondaryCompositeIndex[F[_], P, S]` - represent a secondary index with partition key and sort
  key

The Java AWS SDK client usually just takes a `String` of table's name. However, `meteor` requires
type parameters for table and secondary index, which are type(s) for partition key and sort key.
This is to internally build keys' `AttributeValue` correctly and to deduplicate items in batch
actions.

## Write and Read

It is recommended to use the high level API to interact with a DynamoDB table for simplicity.
Low level API provides a Java wrapper client which has complicated interfaces, however, it is still
useful to use low level API to create, delete or scan a DynamoDB table.

### Using high level API

```scala
import meteor._
import meteor.api.hi._
import cats.effect.{ExitCode, IO, IOApp}

object Main extends IOApp {
  val jClientSrc = 
    Resource.fromAutoCloseable[F, DynamoDbAsyncClient] {
      Sync[F].delay {
        val cred = DefaultCredentialsProvider.create()
        DynamoDbAsyncClient.builder()
          .credentialsProvider(cred)
          .region(Region.EU_WEST_1)
          .build()
      }
    }
  val booksTableSrc = jClientSrc.map { jClient =>
    SimpleTable[IO, Int]("books-table", KeyDef[Int]("id", DynamoDbType.N), jClient)
  }

  val lotr = Book(1, "The Lord of the Rings")

  val found = booksTableSrc.use { table =>
    // To write
    val put = table.put(lotr) // return IO[Unit]
    // To read - eventually consistent
    val get =
      table.get[Book](
        1,
        consistentRead = false
      ) // return IO[Option[Book]]

    put.flatMap(_ => get)
  }

  override def run(args: List[String]): IO[ExitCode] = {
    found.map {
      case Some(book) => IO(println(s"Found $book"))
      case None => IO(println("No book found"))
    }.as(ExitCode.Success)
  }
}
```

### Using low level API

```scala
import meteor._
import cats.effect.{ExitCode, IO, IOApp}

object Main extends IOApp {
  val dynamoClientSrc = Client.resource[IO]
  val booksTable = PartitionKeyTable[Int]("books-table", KeyDef[Int]("id", DynamoDbType.N))

  val lotr = Book(1, "The Lord of the Rings")

  val found = dynamoClientSrc.use { client =>
    // To write
    val put = client.put[Book](bookstable.tableName, lotr) // return IO[Unit]
    // To read - eventually consistent
    val get =
      client.get[Int, Book](
        booksTable,
        1,
        consistentRead = false
      ) // return IO[Option[Book]]

    put.flatMap(_ => get)
  }

  override def run(args: List[String]): IO[ExitCode] = {
    found.map {
      case Some(book) => IO(println(s"Found $book"))
      case None => IO(println("No book found"))
    }.as(ExitCode.Success)
  }
}

```
