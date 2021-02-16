---
title: "Item Actions"
description: ""
lead: ""
date: 2021-01-27T00:17:27Z
lastmod: 2021-01-27T00:17:27Z
draft: false
images: []
menu: 
  docs:
    parent: "api"
weight: 998
toc: true
---

Most actions have several overloading methods for a table with composite keys, or a simple table 
with only partition key.

## Get

Most simple action to get an item from a table by key(s). Since return type `U` is only on the 
return type, all type parameters are required to be passed in explicitly.

```scala
def get[P: Encoder, S: Encoder, U: Decoder](
  table: CompositeKeysTable[P, S],
  partitionKey: P,
  sortKey: S,
  consistentRead: Boolean
): F[Option[U]]
```

## Retrieve

Similar to `Get` but can return multiple items by:

- retrieve multiple items which have the same partition key but different sort key
- retrieve multiple items given a `Query`

### Query

Query can be used to `Retrive` multiple items which fulfills the query's condition.

```scala
case class Query[P: Encoder, S: Encoder](
  partitionKey: P,
  sortKeyQuery: SortKeyQuery[S],
  filter: Expression
)
```

Consider the following dataset:

```markdown
| author - partition key | title - sort key      | publishedAt |
|------------------------|-----------------------|-------------|
| Jules Verne            | Around the Moon       | 1872        |
| Jules Verne            | The Mysterious Island | 1875        |
| Jules Verne            | The Green Ray         | 1882        |
```

To retrieve `The Mysterious Island` item, the query can be:

```scala
import meteor.SortKeyQuery.BeginsWith
import meteor.{Expression, Query}
import meteor.syntax._

Query(
  partitionKey = "Jules Verne",
  sortKeyQuery = BeginsWith("The"),
  filter = Expression(
    expression = "#pAt BETWEEN :from AND :to",
    attributeNames = Map(
      "#pAt" -> "publishedAt"
    ),
    attributeValues = Map(
      ":from" -> 1870.asAttributeValue,
      ":to" -> 1880.asAttributeValue
    )
  )
)
```

When `sortKeyQuery` is unknown, it can be omitted by `SortKeyQuery.empty[P]`. Or when the table has
no `sortKey`, it can be omitted by `SortKeyQuery.empty`, in this case `P`'s type is `Nothing`. 
`filter` can also be omitted by using `Expression.empty`. There are different `apply` methods to
use where the `sortKeyQuery` or `filter` are not required.

In the example `Query` above, please note the usage of `#pAt` and `:from`/`:to`. These are 
[Query syntax](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html), they
are optional, but it is good practice using them to avoid crashes with DynamoDB's reserved words.

## Put

Simple action to insert an item into a table. Currently, supported put actions:

- returns `F[Unit]` - equivalent to `ReturnValue.NONE` in Java AWS SDK.
- returns `F[Option[U]]` - equivalent to `ReturnValue.ALL_OLD` in Java AWS SDK, a `None` is returned
if this is the first item of the given key(s) added to the table.

## Delete

Action to delete a single item from a table. Overriding methods provide deletion functionality for a 
table with composite keys and table with only partition key.

## Update

Action to update a single item on a table. Currently, supported update actions:

- returns `F[Unit]` - equivalent to `ReturnValue.NONE` in Java AWS SDK.
- returns `F[Option[U]]` - depends on the `ReturnValue` argument, `U` represents an item (old or 
  new), or only the updated part of an item (old or new).

Let's consider the most complicated `update` method:

```scala
def update[P: Encoder, S: Encoder, U: Decoder](
  table: CompositeKeysTable[P, S],
  partitionKey: P,
  sortKey: S,
  update: Expression,
  returnValue: ReturnValue
): F[Option[U]]
```

This can be used to update an item of given `partitionKey` and `sortKey` but only update a specific 
attribute(s) (via `update: Expression`), if a certain condition is met (via `condition: Expression`)
and return the old attribute(s) before being updated.

Consider the same dataset in [Query section](./#query). To update `publishedAt` value for 
`The Mysterious Island` to `2021` but only if the current `publishedAt` value is equal to `1875`,
return the old value before being updated:

```scala
import cats.effect.IO
import meteor._
import meteor.syntax._
import software.amazon.awssdk.services.dynamodb.model.ReturnValue

val table = CompositeKeysTable[String, String](
  "books-table",
  KeyDef("author", DynamoDbType.S),
  KeyDef("title", DynamoDbType.S)
)

val client: Client[IO] = ???

val bookPublishedAtOldValue =
  client.update[String, String, Int](
    table,
    "Jules Verne",
    "The Mysterious Island",
    Expression(
      "SET #pAt = :newYear",
      Map("#pAt" -> "publishedAt"),
      Map(":newYear" -> 2021.asAttributeValue)
    ),
    Expression(
      "#pAt = :oldYear",
      Map("#pAt" -> "publishedAt"),
      Map(":newYear" -> 1875.asAttributeValue)
    ),
    ReturnValue.UPDATED_OLD
  ) // returns IO(Some(1875))
```
