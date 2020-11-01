# How to release

Release is done automatically via tags with [this Github Actions](https://github.com/anothrNick/github-tag-action).
Minor version (default) gets bump automatically. Publish to Bintray using [sbt-release-early](https://github.com/jvican/sbt-release-early)
and [sbt-bintray](https://github.com/sbt/sbt-bintray). 

## To release other version bump

Commit message contains `#major` or `#patch`
