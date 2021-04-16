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
`DynamoDbAsyncClient` via `Client.apply` method.

#### Table and Secondary Index

DynamoDB actions can be performed against a table or a secondary index of a table. To do so in 
`meteor`, the table or secondary index needs to be explicitly defined such as:

```scala
import meteor._
val table = PartitionKeyTable[Int]("books-table", KeyDef[Int]("id", DynamoDbType.N))
```

All supported index types are:

- PartitionKeyTable[P]
- PartitionKeySecondaryIndex[P]
- CompositeKeysTable[P, S]
- CompositeKeysSecondaryIndex[P, S]


The Java AWS SDK client usually just takes a `String` of table's name instead. `meteor` requires a 
table instance to build keys' `AttributeValue` correctly and tie it to the definition of the table.
It is also used to deduplicate items in batch actions.

## Write and Read

### Using high level API

```scala
import meteor._
import meteor.api.hi._
import cats.effect.{ExitCode, IO, IOApp}

object Main extends IOApp {
  val dynamoClientSrc = Client.resource[IO]
  val booksTableSrc = dynamoClientSrc.map { client =>
    SimpleTable[IO, Int]("books-table", KeyDef[Int]("id", DynamoDbType.N), client)
  }

  val lotr = Book(1, "The Lord of the Rings")

  val found = booksTableSrc.use { table =>
    // To write
    val put = table.put[Book](lotr) // return IO[Unit]
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
