# meteor

![build](https://github.com/d2a4u/meteor/workflows/build/badge.svg)
[ ![Download](https://api.bintray.com/packages/d2a4u/meteor/meteor-awssdk/images/download.svg) ](https://bintray.com/d2a4u/meteor/meteor-awssdk/_latestVersion)

A Scala wrapper for AWS SDK 2 DynamoDB library using cats effect and fs2.

## Install

Add Bintray resolver:
resolvers += Resolver.bintrayRepo("d2a4u", "meteor")

Add the following to your build.sbt, see the badge above for latest version. Supports Scala 2.12 and 2.13.

libraryDependencies += "meteor" %% "meteor-awssdk" % "LATEST_VERSION"

## Usage and Examples

### Codec

The library provides codec for some primitives out of the box: `String`, `UUID`, `Boolean`, `Long`, 
`Int`, `Instant`, `Option[A]` and `Map[String, A]`.

#### Encoder

```
def write(a: A): AttributeValue
```

A type class defines how to encode type `A` to a Java `AttributeValue`.

#### Decoder

```
def read(av: AttributeValue): Either[DecoderFailure, A]
```

A type class defines how to decode a Java `AttributeValue` to either a `DecoderFailure` or a value 
of type `A`.

### API

The library supports the following DynamoDB's API:

- Get by Primary Key
- Retrieve by Query
- Update
- Delete
- Scan (use `fs2` Stream to scan table)

Please see `ClientSpec.scala` in integration tests for more code example.

## Modules

- `awssdk` the main module for DynamoDB client
- `scanamo` module which provides auto conversion to and from Scanamo's `DynamoFormat` and meteor's 
`Encoder`/`Decoder`

## Credit

The project is inspired by [comms-deduplication](https://github.com/ovotech/comms-deduplication) 
project. Thanks [@filosganga](https://github.com/filosganga) for his contribution and permission to 
use his code in this project.
