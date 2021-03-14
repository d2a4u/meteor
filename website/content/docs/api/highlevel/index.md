---
title: "High Level API"
description: ""
lead: ""
date: 2021-03-06T07:43:39Z
lastmod: 2021-03-06T07:43:39Z
draft: false
images: []
menu: 
  docs:
    parent: ""
weight: 999
toc: true
---

The actions API is considered low level because it is a 1-2-1 mapping to the Java AWS SDK calls.
In contrast, high level API abstracts over DynamoDB tables which makes it simpler to work with.
For example:

**Low level API action:**

```scala
import meteor._
import cats.effect.IO

val client: Client[IO] = ???
val booksTable = PartitionKeyTable[Int]("books-table", KeyDef[Int]("id", DynamoDbType.N))
val lotr = Book(1, "The Lord of the Rings")

val get =
  client.get[Int, Book](
    booksTable,
    1,
    consistentRead = false
  )
```

**High level API table:**

```scala
import meteor._
import meteor.api.hi._
import cats.effect.IO

val client: Client[IO] = ???
val booksTable = SimpleTable[IO, Int]("books-table", KeyDef[Int]("id", DynamoDbType.N), client)
val lotr = Book(1, "The Lord of the Rings")

val put = booksTable.get[Book](1, consistentRead = false)
```

The differences might not look like much, but there are several benefits that high level API 
provides. 

When using low level API actions, it can get confusing sometimes when there are several 
overloaded methods for the same actions to cater use cases for a table with a partition key or for 
a table with composite keys. The high level API removes this confusion by only provide the methods
that are actionable on the table. It also bounds the key's type(s) to table's definition to reduce 
the number of type parameters required. Context bound of `F[_]` is now also on the method's level. 

## Supports

- `SimpleTable` represent a DynamoDB table which only has partition key index.
- `CompositeTable` represent a DynamoDB table which has both partition key and sort key indexes.
- `SimpleIndex` represent a secondary index on a partition key.
- `SecondaryCompositeIndex` represent a secondary index on composite keys.

## Limitations

Table actions and scan actions are not supported in high level API to keep them flexible.
