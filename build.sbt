import sbt.Keys.organization
import sbt.addCompilerPlugin

val catsVersion = "2.8.0"
val catsEffectVersion = "3.3.14"
val fs2Version = "3.2.14"

lazy val dependencies = Seq(
  "org.typelevel" %% "cats-core" % catsVersion,
  "org.typelevel" %% "cats-effect" % catsEffectVersion,
  "co.fs2" %% "fs2-core" % fs2Version,
  "org.scala-lang.modules" %% "scala-collection-compat" % "2.8.1",
  "org.scala-lang.modules" %% "scala-java8-compat" % "1.0.2",
  "software.amazon.awssdk" % "dynamodb" % "2.17.295"
)

lazy val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.2.13",
  "org.scalacheck" %% "scalacheck" % "1.16.0",
  "org.scalatestplus" %% "scalacheck-1-15" % "3.2.11.0"
)

lazy val ItTest = config("it").extend(Test)

lazy val scala213 = "2.13.8"
lazy val scala212 = "2.12.17"

lazy val commonSettings = Seq(
  ThisBuild / organization := "io.github.d2a4u",
  scalaVersion := scala213,
  crossScalaVersions ++= Seq(scala212, scala213),
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
  addCompilerPlugin(
    "org.typelevel" % "kind-projector" % "0.13.2" cross CrossVersion.full
  ),
  Test / scalacOptions ~= filterConsoleScalacOptions,
  Compile / scalacOptions ~= filterConsoleScalacOptions
)

lazy val root = project
  .in(file("."))
  .settings(name := "meteor", commonSettings, noPublish)
  .dependsOn(
    awssdk % "compile->compile;test->test",
    scanamo % "compile->compile;test->test",
    dynosaur % "compile->compile;test->test"
  )
  .aggregate(awssdk, scanamo, dynosaur)

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
      "org.systemfw" %% "dynosaur-core" % "0.4.0"
    ),
    commonSettings
  ).dependsOn(awssdk)

lazy val scanamo = project
  .in(file("scanamo"))
  .settings(
    inConfig(Test)(Defaults.testSettings),
    Test / testOptions += Tests.Argument(
      "-oD"
    ) // enabled time measurement for each test
  )
  .settings(
    name := "meteor-scanamo",
    libraryDependencies ++= dependencies ++ testDependencies.map(
      _ % "test"
    ) ++ Seq(
      "org.scanamo" %% "scanamo" % "1.0.0-M23"
    ),
    commonSettings
  ).dependsOn(awssdk)
