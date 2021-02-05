---
title: "Batch Actions"
description: ""
lead: ""
date: 2021-01-27T00:17:46Z
lastmod: 2021-01-27T00:17:46Z
draft: false
images: []
menu: 
  docs:
    parent: "api"
weight: 999
toc: true
---

All batch APIs require a Java `BackoffStrategy`. A default value is provided by:

```scala
import meteor.Client

Client.BackoffStrategy.default
```

The settings for this default is based on AWS SDK's default for `DynamoDbRetryPolicy`.

## Batch Get

The following scenarios are supported by `batchGet` methods:

- batch get actions across different tables
- batch get where the input keys can fit into memory
- batch get where the input is a `fs2.Stream`

Internally, the library takes care of unprocessed keys, remove duplicated keys within the same 
batch. DynamoDB allows up to 100 keys for `BatchGetItem`, hence, the library uses 100 as batch size.
If you prefer smaller batch size, you can break down the input into smaller batches for in-memory 
input scenarios or control the `maxBatchWait` parameter for stream input.

#### Batch Get Across Tables

```scala
import meteor.Expression
case class BatchGet(
  values: Iterable[AttributeValue],
  consistentRead: Boolean = false,
  projection: Expression = Expression.empty
)

def batchGet(
  requests: Map[String, BatchGet],
  backoffStrategy: BackoffStrategy
): F[Map[String, Iterable[AttributeValue]]]
```

This method gives you the most flexibility where multiple items can be retrieved across different 
tables. Because tables might have different key types, the `BatchGet` request cannot be tied to a 
type, hence, it takes `values: Interable[AttributeValue]` where `AttributeValue` needs to be a map
of key name and key value. For example:

```scala
import meteor.api._
import meteor.codec.Encoder
import meteor.syntax._

case class BookTablePartitionKey(id: String)
case class ExamTableCompositeKey(code: Int, year: Int)

implicit val bookTablePartitionKeyEncoder: Encoder[BookTablePartitionKey] = Encoder.instance { key =>
  Map("id" -> key.id).asAttributeValue
}
implicit val examTableCompositeKeyEncoder: Encoder[ExamTableCompositeKey] = Encoder.instance { key =>
  Map(
    "code" -> key.code,
    "year" -> key.year
  ).asAttributeValue
}
val bookKeys = List(BookTablePartitionKey("1"), BookTablePartitionKey("2")).map(_.asAttributeValue)
val examKeys = List(ExamTableCompositeKey(1, 2020), ExamTableCompositeKey(2, 2021)).map(_.asAttributeValue)
val requests = Map(
  "bookTableName" -> BatchGet(bookKeys),
  "examTableName" -> BatchGet(examKeys)
)

client.batchGet(
  requests,
  Client.BackoffStrategy.default
)
```
As a result, the returned items are represented in as a `Map` of table's name to the items 
associated to the input keys. The user needs to handle decoding of the returned items.

#### Batch Get From The Same Table

These `batchGet` methods only work on a single table but take typed input(s). They are very similar
to [Get item action](../itemactions#get) except that they take multiple keys.

## Batch Write

[integration tests](https://github.com/d2a4u/meteor/blob/master/awssdk/src/it/scala/BatchWriteOpsSpec.scala)