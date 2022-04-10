---
title: "Scanamo Format"
description: ""
lead: ""
date: 2021-01-26T22:19:00Z
lastmod: 2021-01-26T22:19:00Z
draft: false
images: []
menu: 
  docs:
    parent: "codec"
weight: 1002
toc: true
---

The `meteor-scanamo` module provides integration with [Scanamo format library](https://www.scanamo.org/dynamo-format.html).
It provides implicit construction from `scanamo`'s `DynamoFormat` to `meteor`'s codec. 

**Note:** The module uses `scanamo-format`'s version `1.0.0-M11` instead of latest because in my 
experience, this is the most stable version. however, because it is an older version when DynamoDB
did not support empty String, the following scenario: 

- Empty string: `""`
- Optional none: `None`
- Optional some of empty string: `Some("")`

are all serialized to an `AttributeValue` of `null`. This is problematic because once the value is 
written down, reading it back is difficult. The same reason why `meteor` doesn't provide a `Decoder`
instance for `Option[T]` but only provides syntax to help to deal with optional value.

```scala
import meteor.scanamo.formats.conversions._
import meteor.codec.Codec
import org.scanamo.DynamoFormat

implicit val bookFormat: DynamoFormat[Book] = ...
val bookCodec: Codec[Book] = implicitly[Codec[Book]]
```
