---
title: "Dynosaur Schema"
description: ""
lead: ""
date: 2021-01-26T22:19:06Z
lastmod: 2021-01-26T22:19:06Z
draft: false
images: []
menu: 
  docs:
    parent: "codec"
weight: 1001
toc: true
---

The `meteor-dynosaur` module provides integration with [Dynosaur library](https://systemfw.org/dynosaur/#/).
It provides implicit conversion from `Dynosaur`'s schema to `meteor`'s codec. This is an experiment
feature, hence, it is subjected to change.

```scala
import meteor.dynosaur.formats.conversions._
import meteor.Codec

val bookSchema: Schema[Book] = ...
val bookCodec: Codec[Book] = implicitly[Codec[Book]]
```
