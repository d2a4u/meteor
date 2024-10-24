import sbt.Keys.organization

val catsVersion = "2.12.0"
val catsEffectVersion = "3.5.4"
val fs2Version = "3.10.2"

lazy val dependencies = Seq(
  "org.typelevel" %% "cats-core" % catsVersion,
  "org.typelevel" %% "cats-effect" % catsEffectVersion,
  "co.fs2" %% "fs2-core" % fs2Version,
  "org.scala-lang.modules" %% "scala-collection-compat" % "2.12.0",
  "org.scala-lang.modules" %% "scala-java8-compat" % "1.0.2",
  "software.amazon.awssdk" % "dynamodb" % "2.25.70"
)

lazy val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.2.19",
  "org.scalacheck" %% "scalacheck" % "1.18.1",
  "org.scalatestplus" %% "scalacheck-1-16" % "3.2.14.0"
)

lazy val ItTest = config("it").extend(Test)

lazy val scala3 = "3.4.2"
lazy val scala213 = "2.13.14"

lazy val commonSettings = Seq(
  ThisBuild / organization := "io.github.d2a4u",
  scalaVersion := scala3,
  crossScalaVersions += scala213,
  Test / parallelExecution := true,
  scalafmtOnCompile := true,
  licenses += ("MIT", url("http://opensource.org/licenses/MIT")),
  homepage := Some(url("https://d2a4u.github.io/meteor/")),
  publishMavenStyle := true,
  Test / publishArtifact := false,
  pomIncludeRepository := { _ => false },
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/d2a4u/meteor"),
      "git@github.com:d2a4u/meteor.git"
    )
  ),
  developers := List(
    Developer(
      "d2a4u",
      "D A Khu",
      "d2a4u@users.noreply.github.com",
      url("https://github.com/d2a4u")
    )
  ),
  pgpPublicRing := file("/tmp/local.pubring.asc"),
  pgpSecretRing := file("/tmp/local.secring.asc"),
  Global / releaseEarlyWith := SonatypePublisher,
  sonatypeProfileName := "io.github.d2a4u",
  releaseEarlyEnableSyncToMaven := true,
  Test / scalacOptions ~= filterConsoleScalacOptions,
  Compile / scalacOptions ~= filterConsoleScalacOptions
)

lazy val root = project
  .in(file("."))
  .settings(name := "meteor", commonSettings, noPublish)
  .dependsOn(
    awssdk % "compile->compile;test->test",
    dynosaur % "compile->compile;test->test"
  )
  .settings(
    Compile / scalacOptions ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((3, _)) =>
          Seq("-Ykind-projector:underscores")
        case Some((2, 13)) =>
          Seq("-Xsource:3", "-P:kind-projector:underscore-placeholders")
      }
    }
  )
  .aggregate(awssdk, dynosaur)

lazy val noPublish =
  Seq(publish := {}, publishLocal := {}, publishArtifact := false)

lazy val awssdk = project
  .in(file("awssdk"))
  .configs(ItTest)
  .settings(
    inConfig(ItTest)(Defaults.testSettings),
    ItTest / testOptions += Tests.Argument(
      "-oD"
    ) // enabled time measurement for each test
  )
  .settings(
    name := "meteor-awssdk",
    libraryDependencies ++= dependencies ++ testDependencies.map(_ % "it,test"),
    commonSettings
  )

lazy val dynosaur = project
  .in(file("dynosaur"))
  .settings(
    inConfig(Test)(Defaults.testSettings),
    Test / testOptions += Tests.Argument(
      "-oD"
    ) // enabled time measurement for each test
  )
  .settings(
    name := "meteor-dynosaur",
    libraryDependencies ++= dependencies ++ testDependencies.map(
      _ % "test"
    ) ++ Seq(
      "org.systemfw" %% "dynosaur-core" % "0.6.0"
    ),
    commonSettings
  ).dependsOn(awssdk)
