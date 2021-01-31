---
title: "meteor Codec"
description: ""
lead: ""
date: 2021-01-26T22:18:50Z
lastmod: 2021-01-26T22:18:50Z
draft: false
images: []
menu: 
  docs:
    parent: "codec"
weight: 1000
toc: true
---

`meteor` provides `Encoder` and `Decoder` type classes to convert any `A` to and from Java's 
`Attributevalue`. Their signatures are followed:

```scala
trait Encoder[A] {
  def write(a: A): AttributeValue
}

trait Decoder[A] {
  def read(av: AttributeValue): Either[DecoderError, A]
}
```

Please note that the write side is always success, such that, given an `A`, we can always write it 
to an `AttributeValue`. However, reading an `AttributeValue` into `A` might fail as we don't know 
the schema of an `AttributeValue`. 

If you need to create both `Encoder` and `Decoder`, there is a convenient type class `Codec` which
extends both `Encoder` and `Decoder`. Codec for value class can be instantiated using `Codec.iso`:

```scala
case class Id(value: String) extends AnyVal
implicit val idCodec: Codec[Id] = Codec.iso[String, Id](Id.apply)(_.value)
```

## Instances

The library provides built-in instances for primitive and common types. For `Option[T]`, there is an
`Encoder[Option[T]]` but there **is no** `Decoder[Option[T]]`. The reason is that `None` can be
written to an `AttributeValue` of `null`, however, because anything can be `null` in Java, we can't
make the assumption to read `null` as `None`. For example:

```scala
case class Book(id: Int, content: String)
case class Exam(allow: Option[Book])
val exam1 = Exam(Some(null))
val exam2 = Exam(None)
```

User might choose to handle `Encoder[Book]` such that `val book: Book = null` is written as an 
`AttributeValue` of `null`. Hence, if there were a `Decoder[Option[T]]`, `exam1` would be read as 
`exam2` which changed the equality of the original value. It is always recommended to use Scala's 
`Option` over `null`. To help with this issue, the library provides some syntax helpers when reading 
`Option`. Alternatively, you can use [Dynosaur codecs](https://systemfw.org/dynosaur/#/) where it 
enforces a single schema for both write and read.

## Syntax

The following syntax helpers are provided to make conversion to and from `AttributeValue` easier.

#### Encoder Syntax

Convert a primitive value to `AttributeValue` of equivalent type:
```scala
import meteor.syntax._

// from a primitive value to AttributeValue
val intAsAv: AttributeValue = 1.asAttributeValue
```

Convert case class instance to `AttributeValue` as a `Map`:
```scala
// from a case class to AttributeValue
val bookAsAv: AttributeValue = Map(
  "id" -> book.id.asAttributeValue,
  "content" -> book.content.asAttributeValue
).asAttributeValue

val examAsAv: AttributeValue = Map(
  "allow" -> exam.allow.asAttributeValue // require an Encoder[Book] in scope
).asAttributeValue
```

#### Decoder Syntax

Attempt converting `AttributeValue` to a value of given type:

```scala
import meteor.errors.DecoderError
import meteor.syntax._

val int: Either[DecoderError, Int] = intAsAv.as[Int]
val optInt: Either[DecoderError, Option[Int]] = intAsAv.asOpt[Int]

val bookId: Either[DecoderError, Int] = bookAsAv.getAs[Int]("id")
val optBook: Either[DecoderError, Option[Book]] = examAsAv.getOpt[Book]("allow")
```

Note that there are dedicated methods: `asOpt` and `getOpt` to help decoding optional values when
we want to force the behaviour of reading an `AttributeValue` of `null` or when a field is missing 
to Scala's `None`.

## Auto-derivation

Currently, there is no plan to support auto derivation for codec. The reasons are:

* Auto derivation hides how data is being written and can lead to surprises. Let's take `Scanamo`'s 
  auto derivation as an example:

```scala
import org.scanamo._
import org.scanamo.semiauto._

case class Id(value: Int) extends AnyVal
case class Book(id: Id)
implicit val idFormat: DynamoFormat[Id] = deriveDynamoFormat[Id]
implicit val bookFormat: DynamoFormat[Book] = deriveDynamoFormat[Book]
val book1 = Book(Id(1))
```

`book1` is being written into DynamoDB JSON as:

```json
{
  "M": {
    "id": {
      "M": {
        "value": {
          "N": 1
        }
      }
    }
  }
}
```

whereas it can be as simple as:

```json
{
  "M": {
    "id": {
      "N": 1
    }
  }
}
```

* When using auto derivation, conversion is usually required to map between different layers of
  model. With the explicit codec approach, we need fewer layers of model but conversion gets more
  boilerplate. I think the problems are the same with 2 approaches, they just being solved at 
  different places.
  