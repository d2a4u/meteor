---
title: "Expression"
description: ""
lead: ""
date: 2021-01-28T10:14:17Z
lastmod: 2021-01-28T10:14:17Z
draft: false
images: []
menu: 
  docs:
    parent: "api"
weight: 996
toc: true
---

An `Expression` case class represents various expressions in DynamoDB actions including:

- [FilterExpression](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html#DDB-Scan-request-FilterExpression) - Scan API
- [UpdateExpression](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html#DDB-UpdateItem-request-UpdateExpression) - UpdateItem API
- [ConditionExpression](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html#DDB-UpdateItem-request-ConditionExpression) - UpdateItem API 
- [ConditionExpression](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html#DDB-PutItem-request-ConditionExpression) - PutItem API
- [ProjectionExpression](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ProjectionExpressions.html) - GetItem, Query, or Scan API

These expressions in DynamoDB share the same structure, hence, in `meteor` they are abstracted as:

```scala
case class Expression(
  expression: String,
  attributeNames: Map[String, String],
  attributeValues: Map[String, AttributeValue]
)
```

For example:

```scala
Expression(
  expression = "#pAt BETWEEN :from AND :to",
  attributeNames = Map(
    "#pAt" -> "publishedAt"
  ),
  attributeValues = Map(
    ":from" -> 1870.asAttributeValue,
    ":to" -> 1880.asAttributeValue
  )
)
```

where `expression: String` is DynamoDB expression syntax which mirrors Java AWS SDK. `#` and `:` 
prefixes of `pAt`, `from` and `to` are part of the syntax, to avoid crashes with internal DynamoDB 
reserved words. The actual attribute's name and value are replaced by providing the `attributeNames`
and `attributeValues` map respectively.
