---
title: "Build and Tests"
description: ""
lead: ""
date: 2021-01-27T00:39:02Z
lastmod: 2021-01-27T00:39:02Z
draft: false
images: []
menu: 
  docs:
    parent: "introduction"
weight: 902
toc: true
---

## Build

The project requires Scala 2.12.12 or 2.13.3. To build the project locally:

```bash
git@github.com:d2a4u/meteor.git
cd ./meteor
sbt compile
```

## Tests

#### Local

Unit tests can be run locally by `sbt test`. Integration tests run against docker container of 
[DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html).
Hence, you will need docker installed and then to run integration tests:

```bash
docker-compose up -d # make sure that the service is ready before running tests
sbt it:test
```

A `docker-compose.yml` is provided at the root of the project.

#### Github Actions

All PRs and branches are built and tests using Github Actions using the same mechanism with 
`docker-compose`. The project is built and tests are run against both Scala 2.12 and 2.13.

## Release

The project uses [sbt-release-early plugin](https://github.com/jvican/sbt-release-early) and 
[sbt-bintray]() to release artifacts to Bintray repositories using git tags. Release process is 
automated via Github actions. Minor version is automatically incremented on PR merge. To release 
other version bump, add `#major` or `#patch` to commit message.
