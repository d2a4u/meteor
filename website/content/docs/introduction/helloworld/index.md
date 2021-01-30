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

#### Table

Most of the methods provided by `meteor` client take a `Table` where:

```scala
case class Table(
  name: String,
  partitionKey: Key,
  sortKey: Option[Key]
)
```

The Java AWS SDK client usually just takes a `String` of table's name instead. `meteor` requires a 
`Table` to build keys' `AttributeValue` correctly and tie it to the definition of `Table`. It is 
also used to deduplicate items in batch actions.

## Write and Read

```scala
import meteor._
import cats.effect.{ExitCode, IO, IOApp}

object Main extends IOApp {
  val dynamoClientSrc = Client.resource[IO]
  val booksTable = Table("books-table", Key("id", DynamoDbType.N), None)

  val lotr = Book(1, "The Lord of the Rings")

  val found = dynamoClientSrc.use { client =>
    // To write
    val put = client.put[Book](booksTable.name, lotr) // return IO[Unit]
    // To read - eventually consistent
    val get = client.get[Int, Book](booksTable, 1, consistentRead = false) // return IO[Option[Book]]

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
