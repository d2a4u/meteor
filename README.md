# meteor

WIP - A Scala wrapper for AWS SDK 2 DynamoDB library using cats effect and fs2.

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

## Credit

The project is inspired by (comms-deduplication project)[https://github.com/ovotech/comms-deduplication].
Thanks @filosganga for his contribution and permission to use his code in this project.
