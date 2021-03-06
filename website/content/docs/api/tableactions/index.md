---
title: "Table Actions"
description: ""
lead: ""
date: 2021-01-27T00:17:07Z
lastmod: 2021-01-27T00:17:07Z
draft: false
images: []
menu: 
  docs:
    parent: "api"
weight: 999
toc: true
---

## Create

Table creation returns `F[Unit]` where `F[_]` is semantically blocked (no actual JVM thread being 
blocked) until the table has been created and its status is `available`.

```scala
import meteor._

val creation: F[Unit] = client.createPartitionKeyTable(
  "books-table",
  KeyDef[Int]("id", DynamoDbType.N),
  BillingMode.PAY_PER_REQUEST
)
```

`attributeDefinition` is required only when there are secondary indexes on those attributes. For
example, given the following secondary index:

```scala
import software.amazon.awssdk.services.dynamodb.model._

val global2ndIndex = 
  GlobalSecondaryIndex
    .builder()
    .indexName("books-by-author")
    .keySchema(
      KeySchemaElement.builder().attributeName("author").keyType(KeyType.HASH).build(),
      KeySchemaElement.builder().attributeName("title").keyType(KeyType.RANGE).build()
    )
    .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
    .build()
```

Since the index keys are on `"author"` and `"title"` attributes, when creating the table, they need
to be defined:

```scala
import meteor.DynamoDbType

val creation: F[Unit] = client.createPartitionKeyTable[String](
  tableName = "books_table",
  partitionKeyDef = KeyDef[String]("author", DynamoDbType.S),
  billingMode = BillingMode.PAY_PER_REQUEST,
  attributeDefinition = Map(
    "author" -> DynamoDbType.S,
    "title" -> DynamoDbType.S
  ),
  globalSecondaryIndexes = Set(global2ndIndex),
  localSecondaryIndexes = Set.empty
)
```

## Delete

Table deletion also returns `F[Unit]` but it is fire and forget. It returns `Unit` once the 
underline `DeleteTable` request is responded successfully.

```scala
val deletion: F[Unit] = client.deleteTable(table.tableName)
```

## Scan

Scanning a DynamoDB table returns a `fs2.Stream`. It also abstracts away the complexity around
`LastEvaluatedKey` and `Segment`. A filter expression can also be provided.

```scala
val books: Stream[F, Book] = client.scan[Book](
  tableName = table.tableName,
  consistentRead = false,
  parallelism = 32
)
```

