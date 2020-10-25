# meteor

![build](https://github.com/d2a4u/meteor/workflows/build/badge.svg)
[ ![Download](https://api.bintray.com/packages/d2a4u/meteor/meteor-awssdk/images/download.svg) ](https://bintray.com/d2a4u/meteor/meteor-awssdk/_latestVersion)

A Scala wrapper for AWS SDK 2 DynamoDB library using cats effect and fs2.

## Install

Add Bintray resolver:

```
resolvers += Resolver.bintrayRepo("d2a4u", "meteor")
```

Add the following to your build.sbt, see the badge above for latest version. Supports Scala 2.12 and
2.13.

```
libraryDependencies += "meteor" %% "meteor-awssdk" % "LATEST_VERSION"
libraryDependencies += "meteor" %% "meteor-scanamo" % "LATEST_VERSION"
```

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

- Get
- Query
- Put
- Update
- Delete
- Scan (use `fs2` Stream to scan table)
- Batch Update
- Batch Delete

### Usage

Please see [integration tests](https://github.com/d2a4u/meteor/tree/master/awssdk/src/it/scala) 
for more usage example.

## Modules

- `awssdk` the main module for DynamoDB client
- `scanamo` module which provides auto conversion to and from Scanamo's `DynamoFormat` and meteor's 
`Encoder`/`Decoder`

*Note:* `meteor` uses `Scanamo`'s version `1.0.0-M11` instead of latest because in my experience,
this is the most stable version. However, because it is an older version when DynamoDB's did not 
support empty String, this version of `Scanamo` serializes these cases: `""`, `None` and `Some("")`
to Dynamo's `NULL`. This is problematic because once the value is written down, reading it back is
difficult.

## Credit

The project is inspired by [comms-deduplication](https://github.com/ovotech/comms-deduplication) 
project. Thanks [@filosganga](https://github.com/filosganga) for his contribution and permission to 
use his code in this project.
