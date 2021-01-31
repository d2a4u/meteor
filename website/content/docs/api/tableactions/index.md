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
weight: 997
toc: true
---

## Create

Table creation returns `F[Unit]` where `F[_]` is semantically blocked (no actual JVM thread being 
blocked) until the table has been created and its status is `available`. 

```scala
import meteor._

val table = Table("books-table", Key("id", DynamoDbType.N), None)
val creation: F[Unit] = client.createTable(
  table = table,
  attributeDefinition = Map.empty,
  globalSecondaryIndexes = Set.empty,
  localSecondaryIndexes = Set.empty,
  billingMode = BillingMode.PAY_PER_REQUEST
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

val creation: F[Unit] = client.createTable(
  table = table,
  attributeDefinition = Map(
    "author" -> DynamoDbType.S,
    "title" -> DynamoDbType.S
  ),
  globalSecondaryIndexes = Set(global2ndIndex),
  localSecondaryIndexes = Set.empty,
  billingMode = BillingMode.PAY_PER_REQUEST
)
```

## Delete

Table deletion also returns `F[Unit]` but it is fire and forget. It returns `Unit` once the 
underline `DeleteTable` request is responded successfully.

```scala
val deletion: F[Unit] = client.deleteTable(table.name)
```

## Scan

Scanning a DynamoDB table returns a `fs2.Stream`. It also abstracts away the complexity around
`LastEvaluatedKey` and `Segment`. A filter expression can also be provided.

```scala
val books: Stream[F, Book] = client.scan[Book](
  tableName = table.name,
  consistentRead = false,
  parallelism = 32
)
```

