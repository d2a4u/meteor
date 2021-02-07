---
title: "Get Started"
description: ""
lead: ""
date: 2021-01-25T23:54:06Z
lastmod: 2021-01-25T23:54:06Z
draft: false
images: []
menu: 
  docs:
    parent: "introduction"
weight: 900
toc: true
---

`meteor` is a wrapper around Java AWS SDK v2 library, provides higher level
API over standard DynamoDB's operations such as batch write or scan table 
as `fs2` Stream, auto processes left over items and many other features.

## Installation

Add the following to your `build.sbt`, see the badge on homepage for latest version. Supports Scala 
2.12 and 2.13.

```scala
libraryDependencies += "io.github.d2a4u" %% "meteor-awssdk" % "LATEST_VERSION"
```

### Modules

#### [Scanamo Format](https://github.com/scanamo/scanamo)

**Note:** Only version `1.0.0-M11` is supported because in my experience, this is the most stable version of
`Scanamo`. However, because it is an older version when DynamoDB's did not support empty 
String, this version of `Scanamo` serializes these cases: `""`, `None` and `Some("")` to Dynamo's 
`NULL`. This is problematic because once the value is written down, reading it back is difficult.

```scala
libraryDependencies += "io.github.d2a4u" %% "meteor-scanamo" % "LATEST_VERSION"
```

#### [Dynosaur Codecs](https://systemfw.org/dynosaur)

```scala
libraryDependencies += "io.github.d2a4u" %% "meteor-dynosaur" % "LATEST_VERSION"
```

## Fine Tuning

`meteor` uses AWS SDK v2 `DynamoDbAsyncClient` under the hood to make calls to the underline Java
API. The `DynamoDbAsyncClient` internally create a `NettyNioAsyncHttpClient` with a default value
for maximum connections of 50. This can be increased depending on the hardware/virtual machine
configuration. There is also an alternative [AWS CRT HTTP client](https://aws.amazon.com/about-aws/whats-new/2020/09/aws-crt-http-client-in-aws-sdk-for-java-2x/) 
that can be used and tuned similarly. The official documentation can be found [here](https://docs.amazonaws.cn/en_us/sdk-for-java/latest/developer-guide/http-configuration-netty.html).

```scala
import cats.effect._
import meteor.Client
import io.netty.channel.ChannelOption
import software.amazon.awssdk.http.SdkHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

val httpClientSrc = Resource.fromAutoCloseable(
  IO {
    NettyNioAsyncHttpClient
      .builder()
      .putChannelOption(ChannelOption.SO_KEEPALIVE, true)
      .maxConcurrency(100)
      .maxPendingConnectionAcquires(10000).build()
  }
)

def sdkDynamoClientSrc(http: SdkHttpClient) =
  Resource.fromAutoCloseable(
    IO {
      DynamoDbClient.builder().httpClient(http).build()
    }
  )

val clientSrc =
  for {
    http <- httpClientSrc
    sdkClient <-sdkDynamoClientSrc(http)
  } yield Client[IO](sdkClient)
```
